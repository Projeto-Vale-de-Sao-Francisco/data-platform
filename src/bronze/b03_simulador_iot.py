"""Bronze 03 · Leituras IoT simuladas (backfill 2019 → agora) para os 36 sensores do cadastro.

Por que simulado: o backend em produção tem 1 leitura e o Databricks Free Edition não alcança o
Render/ThingSpeak. O simulador reproduz o payload do ESP32 a partir do clima real (Open-Meteo)
+ microclima do talhão + ruído, e injeta de propósito os problemas que a Silver precisa tratar:

  * duplicados (~1,5%) · campos ausentes (~1%) · valores inválidos (999.9, -127, umidade 120, solo -5)
  * extremos (picos de +14 °C, ~0,2%) · quedas de sensor (dias sem dados, sensores offline hoje)
  * datas em 4 formatos: ISO UTC, ISO -03:00, epoch e dd/MM/yyyy local sem fuso
"""
import sys
sys.path.insert(0, "/Volumes/campo_vivo/bronze/code")
from common import (spark, F, com_metadados, salvar, uniforme, normal, clima_horario_base,
                    offset_temp_talhao, alvo_solo_talhao, umidade_solo_expr)

clima = clima_horario_base()
sm_media = clima.groupBy("fazenda_id").agg(F.avg("soil_moisture_0_to_7cm").alias("sm_media"))
sens = spark.table("campo_vivo.bronze.cad_sensores").select(
    F.col("codigo").alias("device"), "talhao_codigo", "fazenda_id", "tipo", "offline_atual")
tal = spark.table("campo_vivo.bronze.cad_talhoes").select(F.col("codigo").alias("talhao_codigo"), "ph_referencia")

base = (sens.join(tal, "talhao_codigo").join(clima, "fazenda_id").join(sm_media, "fazenda_id")
        # instante real da leitura: hora cheia + 0–4 min + segundos (exige reamostragem na Silver)
        .withColumn("ts", F.col("ts") + F.make_interval(F.lit(0), F.lit(0), F.lit(0), F.lit(0), F.lit(0),
                    F.floor(uniforme("device", "ts", k=1) * 5).cast("int"),
                    F.floor(uniforme("device", "ts", k=2) * 60).cast("int")))
        .withColumn("dia", F.to_date("ts")))

# quedas: ~0,8% dos dias por sensor sem nenhuma leitura + sensores offline desde 19/11/2026 05:40
base = base.where(uniforme("device", "dia", k=3) >= 0.008)
base = base.where(~(F.col("offline_atual") & (F.col("ts") >= F.lit("2026-11-19 08:40:00").cast("timestamp"))))

temp = F.col("temperature_2m") + offset_temp_talhao(F.col("talhao_codigo")) + 0.4 * normal("device", "ts", k=4)
solo = umidade_solo_expr(alvo_solo_talhao(F.col("talhao_codigo")), F.col("soil_moisture_0_to_7cm"),
                         F.col("sm_media"), F.col("temperature_2m")) + 1.5 * normal("device", "ts", k=5)
dias_desde_troca = F.datediff("dia", F.lit("2019-01-01")) % 200
bateria = F.round(F.greatest(F.lit(100) - dias_desde_troca * 0.47, F.lit(3)), 0)

u_sujeira = uniforme("device", "ts", k=6)
u_nulo = lambda k: uniforme("device", "ts", k=k) < 0.01

df = (base
      .withColumn("temperatura", F.round(temp, 1))
      .withColumn("umidade", F.round(F.least(F.greatest(F.col("relative_humidity_2m") + 2 * normal("device", "ts", k=7), F.lit(3)), F.lit(100)), 1))
      .withColumn("precipitacao", F.round(F.floor(F.col("precipitation") / 0.2) * 0.2, 1))
      .withColumn("luminosidade", F.round(F.col("shortwave_radiation") * 120, 0))
      .withColumn("umidade_solo", F.round(solo, 1))
      .withColumn("ph", F.round(F.col("ph_referencia") + 0.05 * normal("device", "dia", k=8), 2))
      .withColumn("temperatura_solo", F.round(F.col("temperature_2m") * 0.8 + 4 + 0.3 * normal("device", "ts", k=9), 1))
      .withColumn("bateria", bateria)
      .withColumn("rssi", F.round(-75 + 6 * normal("device", "dia", k=10), 0))
      # inválidos e extremos
      .withColumn("temperatura", F.when(u_sujeira < 0.002, F.lit(999.9))
                  .when(u_sujeira < 0.004, F.lit(-127.0))
                  .when((u_sujeira >= 0.010) & (u_sujeira < 0.012), F.col("temperatura") + 14)
                  .otherwise(F.col("temperatura")))
      .withColumn("umidade", F.when((u_sujeira >= 0.004) & (u_sujeira < 0.006), F.lit(120.0)).otherwise(F.col("umidade")))
      .withColumn("umidade_solo", F.when((u_sujeira >= 0.006) & (u_sujeira < 0.008), F.lit(-5.0)).otherwise(F.col("umidade_solo")))
      # ausentes
      .withColumn("temperatura", F.when(u_nulo(20), None).otherwise(F.col("temperatura")))
      .withColumn("umidade", F.when(u_nulo(21), None).otherwise(F.col("umidade")))
      .withColumn("umidade_solo", F.when(u_nulo(22), None).otherwise(F.col("umidade_solo")))
      # formato de data variável (firmware/gateway diferentes)
      .withColumn("u_fmt", uniforme("device", "dia", k=13))
      .withColumn("ts_txt", F.when(F.col("u_fmt") < 0.70, F.date_format("ts", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
                  .when(F.col("u_fmt") < 0.88, F.date_format(F.col("ts") - F.expr("INTERVAL 3 HOURS"), "yyyy-MM-dd'T'HH:mm:ss'-03:00'"))
                  .when(F.col("u_fmt") < 0.97, F.unix_timestamp("ts").cast("string"))
                  .otherwise(F.date_format(F.col("ts") - F.expr("INTERVAL 3 HOURS"), "dd/MM/yyyy HH:mm:ss")))
      .withColumn("received_at", F.col("ts") + F.make_interval(F.lit(0), F.lit(0), F.lit(0), F.lit(0), F.lit(0), F.lit(0),
                                                              (1 + uniforme("device", "ts", k=14) * 29).cast("int"))))

estacao = F.to_json(F.struct(F.col("device"), F.col("ts_txt").alias("ts"), "temperatura", "umidade", "precipitacao",
                             "luminosidade", "bateria", "rssi"))
sonda = F.to_json(F.struct(F.col("device"), F.col("ts_txt").alias("ts"), "umidade_solo", "ph", "temperatura_solo",
                           "bateria", "rssi"))
saida = (df.withColumn("payload", F.when(F.col("tipo") == "estacao_climatica", estacao).otherwise(sonda))
         .select("payload", "received_at", F.concat(F.lit("GW-F"), F.col("fazenda_id")).alias("gateway"),
                 "device", "ts"))

# duplicados: mesma mensagem reenviada pelo gateway alguns segundos depois
dups = (saida.where(uniforme("device", "ts", k=15) < 0.015)
        .withColumn("received_at", F.col("received_at") + F.expr("INTERVAL 7 SECONDS")))
final = saida.unionByName(dups).drop("device", "ts")

salvar(com_metadados(final, "simulador_iot"), "bronze", "leituras_iot",
       comentario="Mensagens IoT brutas (payload JSON do ESP32). Backfill simulado 2019→agora com sujeira proposital.")
