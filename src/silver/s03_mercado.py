"""Silver 03 · Mercado: preços diários validados, câmbio e tabela de fretes por praça."""
import sys
sys.path.insert(0, "/Volumes/campo_vivo/bronze/code")
from common import spark, F, salvar

b = spark.table("campo_vivo.bronze.mercado_precos_diarios")
b = b.where(F.col("_ingest_ts") == b.agg(F.max("_ingest_ts")).first()[0])
precos = (b.select("cultura", "praca", F.to_date("data").alias("data"), F.col("preco_brl_kg").cast("double"),
                   F.col("usd_brl").cast("double"), "origem_preco", "origem_cambio")
          .where(F.col("preco_brl_kg") > 0)
          .dropDuplicates(["cultura", "praca", "data"]))
salvar(precos, "silver", "mercado_precos", comentario="Preço diário por cultura e praça (R$/kg), validado (> 0, 1 por dia).")

# frete rodoviário/marítimo por kg até cada praça — parâmetros ilustrativos do protótipo (seeds/logistica.json dá o contexto)
fretes = spark.createDataFrame(
    [("Petrolina (PE)", 0.0, "venda local"), ("CEAGESP (SP)", 0.45, "rodoviário refrigerado, 38 h"),
     ("Roterdã (Países Baixos)", 1.10, "rodoviário até Pecém + reefer 11 dias"),
     ("Filadélfia (EUA)", 1.35, "rodoviário até Pecém + reefer"),
     ("Londres (Reino Unido)", 1.20, "rodoviário até Natal + reefer")],
    "praca STRING, frete_brl_kg DOUBLE, rota STRING").withColumn("origem", F.lit("parametro_ilustrativo"))
salvar(fretes, "silver", "mercado_fretes", comentario="Frete estimado por kg até cada praça (parâmetro ilustrativo).")

if spark.catalog.tableExists("campo_vivo.bronze.mercado_comexstat"):
    c = spark.table("campo_vivo.bronze.mercado_comexstat")
    c = c.where(F.col("_ingest_ts") == c.agg(F.max("_ingest_ts")).first()[0])
    salvar(c.where((F.col("kg") > 0) & (F.col("fob_usd") >= 0))
           .groupBy("cultura", "pais", "ano", "mes").agg(F.sum("fob_usd").alias("fob_usd"), F.sum("kg").alias("kg"))
           .withColumn("fob_usd_kg", F.round(F.col("fob_usd") / F.col("kg"), 4))
           .withColumn("data_referencia", F.make_date("ano", "mes", F.lit(1))),
           "silver", "mercado_exportacoes", comentario="Exportações mensais por cultura e país, com preço FOB US$/kg (ComexStat).")
