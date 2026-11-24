"""Gold 01 · Clima diário por talhão, vigor diário e indicadores de produção/colheita (itens 5 e 8)."""
import sys
sys.path.insert(0, "/Volumes/campo_vivo/bronze/code")
from common import spark, F, salvar
from pyspark.sql import Window

TBASE = {"Manga": 13, "Uva": 10, "Goiaba": 12, "Melão": 12}
tbase = F.create_map(*[x for k, v in TBASE.items() for x in (F.lit(k), F.lit(float(v)))])

tal = spark.table("campo_vivo.silver.talhoes").select("talhao_codigo", "cultura")
cul = spark.table("campo_vivo.silver.culturas")
h = (spark.table("campo_vivo.silver.leituras_horarias").join(tal, "talhao_codigo").join(cul, "cultura")
     # hora local (Brasília) para o "dia" agronômico
     .withColumn("data", F.to_date(F.col("hora_utc") - F.expr("INTERVAL 3 HOURS"))))

ref = (spark.table("campo_vivo.silver.clima_horario")
       .withColumn("data", F.to_date(F.col("ts_utc") - F.expr("INTERVAL 3 HOURS")))
       .groupBy("fazenda_id", "data").agg(F.avg("temperatura").alias("temp_ref"), F.sum("chuva_mm").alias("chuva_ref"),
                                          F.avg("et0_mm").alias("et0_ref")))

dentro_t = F.col("temperatura").between(F.col("temperatura_min"), F.col("temperatura_max"))
dentro_u = F.col("umidade").between(F.col("umidade_min"), F.col("umidade_max"))
diario = (h.groupBy("talhao_codigo", "fazenda_id", "cultura", "data")
          .agg(F.round(F.avg("temperatura"), 2).alias("temp_media"), F.max("temperatura").alias("temp_max"),
               F.min("temperatura").alias("temp_min"), F.round(F.avg("umidade"), 1).alias("umidade_media"),
               F.min("umidade").alias("umidade_min"), F.max("umidade").alias("umidade_max"),
               F.round(F.avg("umidade_solo"), 1).alias("umidade_solo_media"), F.round(F.avg("ph"), 2).alias("ph_medio"),
               F.round(F.sum("precipitacao"), 1).alias("chuva_mm"),
               F.sum(F.when(F.col("temperatura").isNotNull() & ~dentro_t, 1).otherwise(0)).alias("horas_temp_fora_faixa"),
               F.sum(F.when(F.col("umidade").isNotNull() & ~dentro_u, 1).otherwise(0)).alias("horas_umid_fora_faixa"),
               F.sum(F.when(dentro_t & dentro_u, 1).otherwise(0)).alias("horas_condicao_adequada"),
               F.sum(F.when(F.col("temperatura") > F.col("temperatura_max"), 1).otherwise(0)).alias("horas_acima_tmax"),
               F.count_if(F.col("temperatura").isNotNull()).alias("horas_com_leitura"))
          .join(ref, ["fazenda_id", "data"], "left")
          # dia sem leitura de temperatura (sensor fora): usa a referência externa e marca a origem
          .withColumn("origem_temp", F.when(F.col("temp_media").isNull(), "referencia_openmeteo").otherwise("sensor"))
          .withColumn("temp_media", F.coalesce("temp_media", F.round("temp_ref", 2)))
          .withColumn("graus_dia", F.round(F.greatest(F.least("temp_media", F.lit(36.0)) - tbase[F.col("cultura")], F.lit(0.0)), 2))
          .withColumn("completude_pct", F.round(F.col("horas_com_leitura") / 24 * 100, 0))
          .drop("temp_ref"))
salvar(diario, "gold", "clima_diario_talhao", comentario="Agregado diário por talhão (dia local), com faixas da cultura e graus-dia.")

# ---------- vigor diário (interpolação linear entre imagens válidas) ----------
vig = spark.table("campo_vivo.silver.vigor")
lim = vig.groupBy("talhao_codigo").agg(F.min("data").alias("ini"), F.max("data").alias("fim"))
dias = lim.select("talhao_codigo", F.explode(F.sequence("ini", "fim")).alias("data"))
v = dias.join(vig.select("talhao_codigo", "data", "indice_vigor"), ["talhao_codigo", "data"], "left")
wa = Window.partitionBy("talhao_codigo").orderBy("data").rowsBetween(Window.unboundedPreceding, 0)
wp = Window.partitionBy("talhao_codigo").orderBy("data").rowsBetween(0, Window.unboundedFollowing)
v = (v.withColumn("d_ant", F.last(F.when(F.col("indice_vigor").isNotNull(), F.col("data")), True).over(wa))
      .withColumn("v_ant", F.last("indice_vigor", True).over(wa))
      .withColumn("d_pos", F.first(F.when(F.col("indice_vigor").isNotNull(), F.col("data")), True).over(wp))
      .withColumn("v_pos", F.first("indice_vigor", True).over(wp))
      .withColumn("gap", F.datediff("d_pos", "d_ant")))
v = v.withColumn("vigor", F.when(F.col("indice_vigor").isNotNull(), F.col("indice_vigor"))
                 .when(F.col("gap") <= 30, F.round(F.col("v_ant") + (F.col("v_pos") - F.col("v_ant")) *
                                                  F.datediff("data", "d_ant") / F.col("gap"), 3)))
salvar(v.select("talhao_codigo", "data", "vigor", F.col("indice_vigor").isNotNull().alias("observado")),
       "gold", "vigor_diario", comentario="Índice de vigor diário interpolado (lacunas <= 30 dias).")

# ---------- indicadores atuais por talhão (item 5) ----------
hoje = F.current_date()
d = spark.table("campo_vivo.gold.clima_diario_talhao")
ult7 = d.where(F.col("data") > F.date_sub(hoje, 7)).groupBy("talhao_codigo").agg(
    F.round(F.sum("horas_condicao_adequada") / F.greatest(F.sum("horas_com_leitura"), F.lit(1)) * 100, 1).alias("indice_condicoes_adequadas_7d"),
    F.sum("horas_temp_fora_faixa").alias("horas_temp_fora_faixa_7d"), F.sum("horas_umid_fora_faixa").alias("horas_umid_fora_faixa_7d"),
    F.round(F.avg("temp_media"), 1).alias("temp_media_7d"), F.round(F.avg("umidade_solo_media"), 1).alias("umidade_solo_7d"))
ant7 = d.where((F.col("data") <= F.date_sub(hoje, 7)) & (F.col("data") > F.date_sub(hoje, 14))).groupBy("talhao_codigo").agg(
    F.round(F.sum("horas_condicao_adequada") / F.greatest(F.sum("horas_com_leitura"), F.lit(1)) * 100, 1).alias("indice_anterior"))
saf = (spark.table("campo_vivo.silver.safras").where("status = 'em_andamento'")
       .select("talhao_codigo", "data_plantio"))
ind = (spark.table("campo_vivo.silver.talhoes").select("talhao_codigo", "fazenda_id", "nome", "cultura", "variedade",
                                                     "previsao_colheita_cadastro")
       .join(ult7, "talhao_codigo", "left").join(ant7, "talhao_codigo", "left").join(saf, "talhao_codigo", "left")
       .withColumn("evolucao_condicoes_pp", F.round(F.col("indice_condicoes_adequadas_7d") - F.col("indice_anterior"), 1))
       .withColumn("tendencia", F.when(F.col("evolucao_condicoes_pp") > 3, "melhorando")
                   .when(F.col("evolucao_condicoes_pp") < -3, "piorando").otherwise("estavel"))
       .withColumn("dias_desde_plantio", F.datediff(hoje, "data_plantio"))
       .withColumn("dias_ate_colheita_cadastro", F.datediff("previsao_colheita_cadastro", hoje))
       .withColumn("atualizado_em", F.current_timestamp())
       .drop("indice_anterior"))
salvar(ind, "gold", "indicadores_talhao", comentario="Indicadores de produção e colheita por talhão (snapshot atual).")
