"""Silver 02 · Tratamento das leituras IoT (item 3 da entrega).

Etapas:
 1. Parse do payload JSON e padronização de datas para UTC (ISO Z, ISO -03:00, epoch, dd/MM/yyyy local).
 2. Deduplicação por (sensor, instante) — fica a primeira mensagem recebida.
 3. Validação física por campo: valores-sentinela de erro (999.9, -127) e fora da faixa viram nulo + motivo.
 4. Extremos: temperatura que diverge > 8 °C do clima de referência (Open-Meteo) na mesma hora;
    umidade do solo fora de Q1-3·IQR / Q3+3·IQR do sensor no mês.
 5. Mensagens sem sensor cadastrado ou sem data legível vão para quarentena.
 6. Reamostragem horária por talhão (média das leituras válidas da hora).
"""
import sys
sys.path.insert(0, "/Volumes/campo_vivo/bronze/code")
from common import spark, F, salvar
from pyspark.sql import Window

raw = spark.table("campo_vivo.bronze.leituras_iot").select("payload", "received_at", "gateway")
if spark.catalog.tableExists("campo_vivo.bronze.leituras_iot_externo"):  # leituras reais (backend/ThingSpeak)
    raw = raw.unionByName(spark.table("campo_vivo.bronze.leituras_iot_externo").select("payload", "received_at", "gateway"))
esquema = ("device STRING, ts STRING, temperatura DOUBLE, umidade DOUBLE, precipitacao DOUBLE, luminosidade DOUBLE, "
           "umidade_solo DOUBLE, ph DOUBLE, temperatura_solo DOUBLE, bateria DOUBLE, rssi DOUBLE")
p = raw.withColumn("j", F.from_json("payload", esquema)).select("payload", "received_at", "gateway", "j.*")

# 1) datas → UTC
ts = F.col("ts")
p = (p.withColumn("formato_data", F.when(ts.rlike(r"^\d{9,11}$"), "epoch")
                  .when(ts.rlike(r"Z$"), "iso_utc")
                  .when(ts.rlike(r"[+-]\d{2}:\d{2}$"), "iso_offset")
                  .when(ts.rlike(r"^\d{2}/\d{2}/\d{4} "), "local_sem_fuso")
                  .otherwise("desconhecido"))
     .withColumn("ts_utc", F.when(F.col("formato_data") == "epoch", F.to_timestamp(F.from_unixtime(ts.cast("long"))))
                 .when(F.col("formato_data").isin("iso_utc", "iso_offset"), F.to_timestamp(ts))
                 # firmware antigo grava hora de Brasília sem fuso: soma 3 h
                 .when(F.col("formato_data") == "local_sem_fuso",
                       F.to_timestamp(ts, "dd/MM/yyyy HH:mm:ss") + F.expr("INTERVAL 3 HOURS"))))

sens = spark.table("campo_vivo.silver.sensores").select(F.col("sensor_codigo").alias("device"), "talhao_codigo", "fazenda_id", "tipo")
p = p.join(sens, "device", "left")

motivo_quarentena = (F.when(F.col("device").isNull(), "payload_ilegivel")
                     .when(F.col("talhao_codigo").isNull(), "sensor_nao_cadastrado")
                     .when(F.col("ts_utc").isNull(), "data_ilegivel")
                     .when(F.col("ts_utc") > F.col("received_at") + F.expr("INTERVAL 10 MINUTES"), "data_no_futuro"))
p = p.withColumn("motivo_quarentena", motivo_quarentena)
quarentena = p.where(F.col("motivo_quarentena").isNotNull()).select("payload", "received_at", "gateway", "motivo_quarentena")
ok = p.where(F.col("motivo_quarentena").isNull())

# 2) duplicados
total_ok = ok.count()
w = Window.partitionBy("device", "ts_utc").orderBy("received_at")
ok = ok.withColumn("_rn", F.row_number().over(w))
duplicados = ok.where("_rn > 1").count()
ok = ok.where("_rn = 1").drop("_rn")

# 3) validação física por campo
REGRAS = {  # campo: (mínimo, máximo)
    "temperatura": (-10, 55), "umidade": (0, 100), "umidade_solo": (0, 100), "ph": (3, 10),
    "temperatura_solo": (-5, 70), "precipitacao": (0, 200), "luminosidade": (0, 200000),
}
motivos = []
for campo, (lo, hi) in REGRAS.items():
    ok = ok.withColumn(f"_f_{campo}_invalido", F.col(campo).isNotNull() & ~F.col(campo).between(lo, hi))
    motivos.append(F.when(F.col(f"_f_{campo}_invalido"), F.lit(f"{campo}_invalido")))
    ok = ok.withColumn(campo, F.when(F.col(f"_f_{campo}_invalido"), None).otherwise(F.col(campo)))

# 4) extremos — temperatura vs referência externa na mesma hora
clima = spark.table("campo_vivo.silver.clima_horario").select(
    "fazenda_id", F.col("ts_utc").alias("hora_utc"), F.col("temperatura").alias("temp_ref"))
ok = ok.withColumn("hora_utc", F.date_trunc("hour", "ts_utc")).join(clima, ["fazenda_id", "hora_utc"], "left")
ok = ok.withColumn("_f_t_ext", F.col("temperatura").isNotNull() & F.col("temp_ref").isNotNull() & (F.abs(F.col("temperatura") - F.col("temp_ref")) > 8))
motivos.append(F.when(F.col("_f_t_ext"), F.lit("temperatura_extremo")))
ok = ok.withColumn("temperatura", F.when(F.col("_f_t_ext"), None).otherwise(F.col("temperatura")))

#    umidade do solo — IQR por sensor e mês
ok = ok.withColumn("mes", F.date_trunc("month", "ts_utc"))
q = ok.groupBy("device", "mes").agg(F.percentile_approx("umidade_solo", 0.25).alias("q1"),
                                    F.percentile_approx("umidade_solo", 0.75).alias("q3"))
ok = ok.join(q, ["device", "mes"], "left")
iqr = F.col("q3") - F.col("q1")
ok = ok.withColumn("_f_s_ext", F.col("umidade_solo").isNotNull() & ((F.col("umidade_solo") < F.col("q1") - 3 * iqr) | (F.col("umidade_solo") > F.col("q3") + 3 * iqr)))
motivos.append(F.when(F.col("_f_s_ext"), F.lit("umidade_solo_extremo")))
ok = ok.withColumn("umidade_solo", F.when(F.col("_f_s_ext"), None).otherwise(F.col("umidade_solo")))

# 5) ausentes (campo esperado para o tipo de sensor e nulo no payload original)
ausente_est = (F.col("tipo") == "estacao_climatica") & (F.col("j_temp_nula") | F.col("j_umid_nula"))
ok = ok.withColumn("j_temp_nula", F.get_json_object("payload", "$.temperatura").isNull()) \
       .withColumn("j_umid_nula", F.get_json_object("payload", "$.umidade").isNull()) \
       .withColumn("j_solo_nula", F.get_json_object("payload", "$.umidade_solo").isNull())
motivos.append(F.when(ausente_est, F.lit("campo_ausente")))
motivos.append(F.when((F.col("tipo") == "sonda_solo") & F.col("j_solo_nula"), F.lit("campo_ausente")))

ok = ok.withColumn("problemas", F.filter(F.array(*motivos), lambda x: x.isNotNull()))
validas = ok.select("device", "talhao_codigo", "fazenda_id", "tipo", "ts_utc", "hora_utc", "formato_data", "received_at",
                    "temperatura", "umidade", "precipitacao", "luminosidade", "umidade_solo", "ph", "temperatura_solo",
                    "bateria", "rssi", "problemas").withColumnRenamed("device", "sensor_codigo")

salvar(validas, "silver", "leituras", comentario="Leituras IoT tratadas: UTC, sem duplicados, inválidos/extremos anulados com motivo.")
salvar(quarentena, "silver", "leituras_quarentena", comentario="Mensagens rejeitadas com motivo.")

# 6) reamostragem horária por talhão (estação + sonda numa linha)
lv = spark.table("campo_vivo.silver.leituras")
horario = (lv.groupBy("talhao_codigo", "fazenda_id", "hora_utc")
           .agg(*[F.round(F.avg(c), 2).alias(c) for c in ["temperatura", "umidade", "umidade_solo", "ph", "luminosidade", "temperatura_solo"]],
                F.round(F.max("precipitacao"), 1).alias("precipitacao"),
                F.countDistinct("sensor_codigo").alias("sensores_ativos")))
salvar(horario, "silver", "leituras_horarias", comentario="Leituras reamostradas por talhão e hora (UTC).")

# relatório de qualidade da carga
prob = (lv.select(F.explode("problemas").alias("problema")).groupBy("problema").count())
resumo = (prob.unionByName(spark.createDataFrame([("duplicado_removido", duplicados)], "problema STRING, count LONG"))
          .unionByName(quarentena.groupBy(F.col("motivo_quarentena").alias("problema")).count())
          .unionByName(lv.groupBy(F.concat(F.lit("formato_data:"), "formato_data").alias("problema")).count())
          .withColumn("total_mensagens", F.lit(raw.count())).withColumn("gerado_em", F.current_timestamp()))
salvar(resumo, "silver", "qualidade_leituras", comentario="Contagem de problemas tratados na última carga.")
resumo.orderBy(F.desc("count")).show(30, False)
