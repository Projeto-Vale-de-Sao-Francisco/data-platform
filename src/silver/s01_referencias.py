"""Silver 01 · Cadastros, clima, safras e vigor tipados e deduplicados."""
import sys
sys.path.insert(0, "/Volumes/campo_vivo/bronze/code")
from common import spark, F, salvar, clima_horario_base
from pyspark.sql import Window

# ---------- cadastros (último snapshot) ----------
def ultimo(nome):
    df = spark.table(f"campo_vivo.bronze.cad_{nome}")
    return df.where(F.col("_ingest_ts") == df.agg(F.max("_ingest_ts")).first()[0]).drop("_arquivo")

salvar(ultimo("fazendas").select(F.col("id").cast("int").alias("fazenda_id"), "nome", "municipio", "uf",
       F.col("area_total_ha").cast("double"), "produtor", "latitude", "longitude"), "silver", "fazendas")
salvar(ultimo("culturas").select(F.col("nome").alias("cultura"), "variedades",
       *[F.col(c).cast("double").alias(c) for c in ["temperatura_min", "temperatura_max", "umidade_min", "umidade_max"]],
       F.col("ciclo_medio_dias").cast("int"), "descricao"), "silver", "culturas")
salvar(ultimo("talhoes").select(F.col("codigo").alias("talhao_codigo"), F.col("fazenda_id").cast("int"), "nome",
       "cultura", "variedade", F.to_date("data_plantio_atual").alias("data_plantio_atual"),
       F.to_date("previsao_colheita_atual").alias("previsao_colheita_cadastro"), "estagio", "poligono",
       F.col("ph_referencia").cast("double")), "silver", "talhoes")
salvar(ultimo("sensores").select(F.col("codigo").alias("sensor_codigo"), "talhao_codigo", F.col("fazenda_id").cast("int"),
       "tipo", "latitude", "longitude", F.col("offline_atual").cast("boolean"), F.col("bateria").cast("int"),
       F.col("sinal_dbm").cast("int"), "firmware"), "silver", "sensores")

# ---------- clima horário (UTC, nomes em português, sem duplicidade de ingestão) ----------
clima = clima_horario_base().select(
    "fazenda_id", F.col("ts").alias("ts_utc"), "origem",
    F.col("temperature_2m").alias("temperatura"), F.col("relative_humidity_2m").alias("umidade"),
    F.col("precipitation").alias("chuva_mm"), F.col("soil_moisture_0_to_7cm").alias("umidade_solo_m3m3"),
    F.col("shortwave_radiation").alias("radiacao_wm2"), F.col("et0_fao_evapotranspiration").alias("et0_mm"),
    F.col("wind_speed_10m").alias("vento_kmh"))
salvar(clima, "silver", "clima_horario", comentario="Clima de referência por fazenda (Open-Meteo), 1 linha por hora UTC.")

# ---------- previsão diária: último boletim emitido ----------
prev = spark.table("campo_vivo.bronze.clima_previsao_diaria")
w = Window.partitionBy("fazenda_id", "data").orderBy(F.col("emitida_em").desc())
salvar(prev.withColumn("_rn", F.row_number().over(w)).where("_rn=1")
       .select("fazenda_id", F.to_date("data").alias("data"), "emitida_em",
               F.col("temperature_2m_max").alias("temp_max"), F.col("temperature_2m_min").alias("temp_min"),
               F.col("precipitation_sum").alias("chuva_prevista_mm"),
               F.col("precipitation_probability_max").alias("prob_chuva_pct"), F.col("et0_fao_evapotranspiration").alias("et0_mm")),
       "silver", "previsao_diaria")

# ---------- safras ----------
saf = spark.table("campo_vivo.bronze.safras_historico")
saf = saf.where(F.col("_ingest_ts") == saf.agg(F.max("_ingest_ts")).first()[0])
salvar(saf.select("talhao_codigo", F.col("safra_seq").cast("int"), "cultura", "variedade", "status",
                  *[F.to_date(c).alias(c) for c in ["data_plantio", "data_inicio_colheita", "data_fim_colheita",
                                                    "data_inicio_exportacao", "data_fim_exportacao"]],
                  F.col("produtividade_t_ha").cast("double"), "destino_exportacao")
       .withColumn("safra_id", F.concat_ws("-", "talhao_codigo", F.lpad(F.col("safra_seq").cast("string"), 2, "0")))
       .withColumn("dias_ate_colheita", F.datediff("data_inicio_colheita", "data_plantio"))
       .withColumn("duracao_colheita", F.datediff("data_fim_colheita", "data_inicio_colheita"))
       .withColumn("produtividade_valida", F.col("produtividade_t_ha").between(1, 120) | F.col("produtividade_t_ha").isNull()),
       "silver", "safras")

# ---------- vigor: remove imagens com nuvem (>50%) e NDVI nulo/fora de [0,1] ----------
vig = spark.table("campo_vivo.bronze.vigor_satelite")
vig = vig.where(F.col("_ingest_ts") == vig.agg(F.max("_ingest_ts")).first()[0])
salvar(vig.where((F.col("cobertura_nuvens_pct") <= 50) & F.col("ndvi").between(0, 1))
       .select("talhao_codigo", F.to_date("data_imagem").alias("data"), F.col("ndvi").cast("double").alias("indice_vigor"),
               F.col("cobertura_nuvens_pct").cast("double")),
       "silver", "vigor", comentario="Índice de vigor válido (NDVI, nuvem <= 50%).")
