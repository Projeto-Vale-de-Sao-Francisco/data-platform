"""ML 02 · Risco climático dos próximos 7 dias por talhão e por fazenda (regra sobre a previsão Open-Meteo).

Por talhão: dias de calor = média diária prevista acima do teto da cultura (+1 °C) OU máxima prevista acima do
percentil 90 histórico (2019→) da fazenda no mesmo mês; dias de frio = média abaixo do piso da cultura (−1 °C);
chuva forte (>= 20 mm/dia) quando a colheita prevista cai nos próximos 21 dias (risco de rachadura/podridão).
  pontos = 2·dias_calor + 2·dias_frio + 3·dias_chuva_forte_na_colheita
  Baixo (0–2) · Moderado (3–7) · Alto (>= 8)
"""
import sys
sys.path.insert(0, "/Volumes/campo_vivo/bronze/code")
from common import spark, F, salvar

hoje = F.current_date()
prev = spark.table("campo_vivo.silver.previsao_diaria").where((F.col("data") > hoje) & (F.col("data") <= F.date_add(hoje, 7)))
tal = spark.table("campo_vivo.silver.talhoes").select("talhao_codigo", "fazenda_id", "nome", "cultura")
cul = spark.table("campo_vivo.silver.culturas").select("cultura", "temperatura_min", "temperatura_max")
pred = spark.table("campo_vivo.gold.predicoes_talhao").select("talhao_codigo", "colheita_inicio")
clim = (spark.table("campo_vivo.silver.clima_horario")
        .withColumn("data", F.to_date(F.col("ts_utc") - F.expr("INTERVAL 3 HOURS")))
        .groupBy("fazenda_id", "data").agg(F.max("temperatura").alias("tmax"))
        .groupBy("fazenda_id", F.month("data").alias("mes")).agg(F.percentile_approx("tmax", 0.9).alias("tmax_p90_hist")))
prev = (prev.withColumn("mes", F.month("data")).join(clim, ["fazenda_id", "mes"], "left")
        .withColumn("temp_media_prev", (F.col("temp_max") + F.col("temp_min")) / 2))

r = (tal.join(cul, "cultura").join(pred, "talhao_codigo", "left").join(prev, "fazenda_id")
     .groupBy("talhao_codigo", "fazenda_id", "nome", "cultura", "temperatura_max", "colheita_inicio")
     .agg(F.count_if((F.col("temp_media_prev") > F.col("temperatura_max") + 1) |
                     (F.col("temp_max") > F.col("tmax_p90_hist"))).alias("dias_calor"),
          F.count_if(F.col("temp_media_prev") < F.col("temperatura_min") - 1).alias("dias_frio"),
          F.round(F.max("tmax_p90_hist"), 1).alias("tmax_p90_historico"),
          F.round(F.max("temp_max"), 1).alias("temp_max_prevista"),
          F.round(F.sum("chuva_prevista_mm"), 1).alias("chuva_7d_mm"),
          F.count_if(F.col("chuva_prevista_mm") >= 20).alias("dias_chuva_forte"))
     .withColumn("colheita_proxima", F.datediff("colheita_inicio", hoje) <= 21)
     .withColumn("pontos", 2 * F.col("dias_calor") + 2 * F.col("dias_frio") +
                 3 * F.when(F.col("colheita_proxima"), F.col("dias_chuva_forte")).otherwise(0))
     .withColumn("nivel", F.when(F.col("pontos") >= 8, "Alto").when(F.col("pontos") >= 3, "Moderado").otherwise("Baixo"))
     .withColumn("motivo", F.concat_ws("; ",
                 F.when(F.col("dias_calor") > 0, F.format_string("%d dia(s) de calor acima do normal (máx. prevista %.1f °C; p90 histórico %.1f °C)", "dias_calor", "temp_max_prevista", "tmax_p90_historico")),
                 F.when(F.col("dias_frio") > 0, F.format_string("%d dia(s) abaixo da mínima", "dias_frio")),
                 F.when(F.col("colheita_proxima") & (F.col("dias_chuva_forte") > 0), F.lit("chuva forte prevista perto da colheita"))))
     .withColumn("gerado_em", F.current_timestamp()))
salvar(r, "gold", "risco_climatico_talhao", comentario="Risco climático 7 dias por talhão (regra sobre a previsão).")

ordem = F.create_map(F.lit("Baixo"), F.lit(0), F.lit("Moderado"), F.lit(1), F.lit("Alto"), F.lit(2))
fz = (r.withColumn("ord", ordem[F.col("nivel")]).groupBy("fazenda_id")
      .agg(F.max("ord").alias("ord"),
           F.concat_ws(", ", F.sort_array(F.collect_list(F.when(F.col("nivel") != "Baixo", F.col("talhao_codigo"))))).alias("talhoes_em_risco"))
      .withColumn("nivel", F.element_at(F.array(F.lit("Baixo"), F.lit("Moderado"), F.lit("Alto")), F.col("ord") + 1))
      .withColumn("resumo", F.when(F.col("talhoes_em_risco") != "", F.concat(F.col("talhoes_em_risco"), F.lit(" acima da faixa de temperatura ou com chuva na colheita")))
                  .otherwise(F.lit("Sem risco relevante previsto")))
      .drop("ord").withColumn("gerado_em", F.current_timestamp()))
salvar(fz, "gold", "risco_climatico_fazenda", comentario="Risco climático 7 dias por fazenda (pior talhão).")
r.select("talhao_codigo", "cultura", "temp_max_prevista", "chuva_7d_mm", "pontos", "nivel", "motivo").orderBy("talhao_codigo").show(20, False)
fz.show(truncate=False)
