"""Bronze 01 · Cadastros (fazendas, talhões, sensores, culturas, produtores, mercado, logística).

Fonte: arquivos JSON Lines em /Volumes/campo_vivo/bronze/landing/cadastros/, enviados pelo extrator
externo (hoje gerados a partir do cadastro de demonstração do frontend). Snapshot completo a cada carga.
"""
import sys
sys.path.insert(0, "/Volumes/campo_vivo/bronze/code")
from common import spark, F, LANDING, com_metadados, salvar

ENTIDADES = ["fazendas", "talhoes", "sensores", "culturas", "produtores", "mercado", "logistica"]

for nome in ENTIDADES:
    df = (spark.read.format("json")
          .option("primitivesAsString", "false")
          .load(f"{LANDING}/cadastros/{nome}.json")
          .withColumn("_arquivo", F.col("_metadata.file_path")))
    if "poligono" in df.columns:
        df = df.withColumn("poligono", F.to_json("poligono"))
    salvar(com_metadados(df, "cadastro_frontend"), "bronze", f"cad_{nome}",
           comentario=f"Cadastro bruto de {nome} (snapshot).")
