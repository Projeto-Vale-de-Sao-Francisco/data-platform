"""Bronze 05 · Leituras reais enviadas pelo extrator externo (backend Render + ThingSpeak).

Auto Loader incremental (availableNow): cada arquivo JSON Lines que chega em landing/iot_externo/ é lido
uma única vez (checkpoint no Volume) e acrescentado em bronze.leituras_iot_externo.
"""
import os
import sys
sys.path.insert(0, "/Volumes/campo_vivo/bronze/code")
from common import spark, F, LANDING

ORIGEM = f"{LANDING}/iot_externo"
CKPT = f"{LANDING}/_checkpoints/iot_externo"
os.makedirs(ORIGEM, exist_ok=True)

stream = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "json")
          .option("cloudFiles.schemaLocation", f"{CKPT}/schema")
          .schema("payload STRING, gateway STRING, received_at STRING")
          .load(ORIGEM)
          .select("payload", F.to_timestamp("received_at").alias("received_at"), "gateway",
                  F.current_timestamp().alias("_ingest_ts"), F.lit("extrator_externo").alias("_source"),
                  F.col("_metadata.file_path").alias("_arquivo")))
q = (stream.writeStream.option("checkpointLocation", CKPT).trigger(availableNow=True)
     .toTable("campo_vivo.bronze.leituras_iot_externo"))
q.awaitTermination()
print("[ok] campo_vivo.bronze.leituras_iot_externo:", spark.table("campo_vivo.bronze.leituras_iot_externo").count(), "linhas")
