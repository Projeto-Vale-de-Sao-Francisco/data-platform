"""Utilidades compartilhadas pelos scripts do pipeline Campo Vivo (Databricks serverless)."""
from pyspark.sql import SparkSession, DataFrame, functions as F

CATALOGO = "campo_vivo"
LANDING = f"/Volumes/{CATALOGO}/bronze/landing"
INICIO_HISTORICO = "2019-01-01"

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")


def tabela(camada: str, nome: str) -> str:
    return f"{CATALOGO}.{camada}.{nome}"


def com_metadados(df: DataFrame, origem: str) -> DataFrame:
    """Colunas de linhagem exigidas em toda tabela Bronze."""
    return df.withColumn("_ingest_ts", F.current_timestamp()).withColumn("_source", F.lit(origem))


def salvar(df: DataFrame, camada: str, nome: str, modo: str = "overwrite", comentario: str | None = None):
    alvo = tabela(camada, nome)
    (df.write.format("delta").mode(modo).option("overwriteSchema", "true").saveAsTable(alvo))
    if comentario:
        spark.sql(f"COMMENT ON TABLE {alvo} IS '{comentario.replace(chr(39), '')}'")
    print(f"[ok] {alvo}: {spark.table(alvo).count()} linhas")


def uniforme(*cols, k: int = 0):
    """Número pseudoaleatório determinístico em [0,1) a partir de colunas (reprodutível entre execuções)."""
    h = F.xxhash64(*cols, F.lit(k))
    return (F.pmod(h, F.lit(1_000_003)) / F.lit(1_000_003.0))


def clima_horario_base():
    """Clima horário 'verdadeiro' por fazenda: archive + forecast (só horas após o fim do archive e até agora)."""
    h = spark.table("campo_vivo.bronze.clima_openmeteo_horario")
    ult = h.where("origem='archive'").groupBy("fazenda_id").agg(F.max("time").alias("fim_archive"))
    h = (h.join(ult, "fazenda_id")
         .where((F.col("origem") == "archive") | ((F.col("time") > F.col("fim_archive")) &
                (F.to_timestamp("time") <= F.current_timestamp())))
         .withColumn("ts", F.to_timestamp("time")))
    # se houver mais de uma ingestão, fica a mais recente por (fazenda, hora)
    from pyspark.sql import Window
    w = Window.partitionBy("fazenda_id", "ts").orderBy(F.col("_ingest_ts").desc())
    return h.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn", "fim_archive")


# Microclima do talhão (determinístico): mesmo valor no simulador IoT e no gerador de safras.
def offset_temp_talhao(col):
    return 0.8 * normal(col, k=11)


def alvo_solo_talhao(col):
    return 36 + 9 * uniforme(col, k=12)


def umidade_solo_expr(alvo, sm, sm_media, temp):
    """Umidade do solo (%) em pomar irrigado: alvo de manejo + sinal do clima - estresse de calor."""
    return F.least(F.greatest(alvo + 55 * (sm - sm_media) - 0.25 * F.greatest(temp - 32, F.lit(0)), F.lit(5)), F.lit(60))


def normal(*cols, k: int = 0):
    """Ruído N(0,1) determinístico via Box-Muller."""
    u1 = F.greatest(uniforme(*cols, k=k), F.lit(1e-9))
    u2 = uniforme(*cols, k=k + 7919)
    return F.sqrt(-2 * F.log(u1)) * F.cos(2 * 3.141592653589793 * u2)
