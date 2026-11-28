"""Bronze 06 · Mercado: ComexStat e PTAX reais (quando o extrator já enviou) + série diária de preços por praça.

Fontes reais (landing/mercado/, enviadas pelo GitHub Actions — o Databricks Free Edition não alcança essas APIs):
  * comexstat_*.json → valor FOB (US$) e peso (kg) exportados por país e mês, NCMs de manga, uva, melão e goiaba, PE/BA/RN
  * ptax_*.json      → dólar PTAX de venda, diário

Série diária por cultura × praça (2019 → hoje): não existe fonte pública diária de preço por praça, então ela é
SIMULADA e identificada em `_source`:
  * nível = preço-base da cultura × fator da praça (os mesmos do protótipo)
  * sazonalidade = preço cai no pico de colheita do Vale (oferta)
  * oferta local = talhões em colheita no dia, a partir de silver.safras quando existir
  * praças de exportação: forma mensal ancorada no FOB/kg real da ComexStat do país e no câmbio PTAX real,
    quando disponíveis (`simulado_ancorado_comexstat`); sem eles, só sazonalidade + câmbio (`simulado`)
  * ruído AR(1) diário
"""
import glob
import json
import sys
import zlib
import datetime as dt
import numpy as np
import pandas as pd
sys.path.insert(0, "/Volumes/campo_vivo/bronze/code")
from common import spark, F, LANDING, com_metadados, salvar

PASTA = f"{LANDING}/mercado"
HOJE = pd.Timestamp(dt.date.today())
INICIO = pd.Timestamp("2019-01-01")
BASE = {"Manga": 4.8, "Uva": 9.6, "Goiaba": 3.2, "Melão": 2.9}  # R$/kg em Petrolina (protótipo)
PRACAS = {  # fator sobre a base, país na ComexStat
    "Petrolina (PE)": (1.00, None), "CEAGESP (SP)": (1.18, None),
    "Roterdã (Países Baixos)": (1.62, "Países Baixos"), "Filadélfia (EUA)": (1.75, "Estados Unidos"),
    "Londres (Reino Unido)": (1.68, "Reino Unido"),
}
# mês de preço mais baixo (pico de oferta no Vale) e amplitude sazonal
SAZONAL = {"Manga": (10.5, 0.16), "Uva": (11.5, 0.14), "Goiaba": (8.0, 0.08), "Melão": (11.0, 0.12)}
PTAX_ANUAL_APROX = {2019: 3.95, 2020: 5.15, 2021: 5.40, 2022: 5.16, 2023: 4.99, 2024: 5.39, 2025: 5.55, 2026: 5.40}


def mais_recente(prefixo):
    arqs = sorted(glob.glob(f"{PASTA}/{prefixo}_*.json"))
    return arqs[-1] if arqs else None


# ---------------- reais: ComexStat ----------------
arq = mais_recente("comexstat")
comex = pd.DataFrame()
if arq:
    js = json.load(open(arq))
    linhas = []
    for cultura, bloco in js.items():
        for r in bloco["resposta"].get("data", {}).get("list", []):
            try:
                linhas.append(dict(cultura=cultura, ncm=",".join(bloco["ncm"]), pais=str(r.get("country", "")).strip(),
                                   ano=int(r["year"]), mes=int(r["monthNumber"]),
                                   fob_usd=float(r["metricFOB"]), kg=float(r["metricKG"])))
            except (KeyError, TypeError, ValueError):
                continue
    comex = pd.DataFrame(linhas)
    print(f"ComexStat: {len(comex)} linhas de {arq.rsplit('/', 1)[-1]}")
    if len(comex):
        salvar(com_metadados(spark.createDataFrame(comex), "comexstat").withColumn("_arquivo", F.lit(arq)),
               "bronze", "mercado_comexstat", comentario="Exportações por país e mês (ComexStat/MDIC), NCMs das culturas, PE/BA/RN.")
else:
    print("ComexStat: nenhum arquivo em landing/mercado ainda (o extrator roda no GitHub Actions)")

# ---------------- reais: PTAX ----------------
arq = mais_recente("ptax")
ptax = pd.DataFrame()
if arq:
    v = json.load(open(arq)).get("value", [])
    ptax = pd.DataFrame([dict(data=str(x["dataHoraCotacao"])[:10], usd_brl=float(x["cotacaoVenda"]))
                         for x in v if x.get("cotacaoVenda")])
    print(f"PTAX: {len(ptax)} cotações de {arq.rsplit('/', 1)[-1]}")
    if len(ptax):
        salvar(com_metadados(spark.createDataFrame(ptax), "bcb_ptax").withColumn("_arquivo", F.lit(arq)),
               "bronze", "cambio_ptax", comentario="Dólar PTAX de venda, diário (Banco Central).")
else:
    print("PTAX: nenhum arquivo em landing/mercado ainda")

# ---------------- série diária simulada ----------------
dias = pd.date_range(INICIO, HOJE, freq="D")
if len(ptax):
    cambio = ptax.assign(data=pd.to_datetime(ptax.data)).groupby("data").usd_brl.last().reindex(dias).ffill().bfill()
    origem_cambio = "real"
else:
    anual = pd.Series({pd.Timestamp(f"{a}-07-01"): v for a, v in PTAX_ANUAL_APROX.items()})
    cambio = anual.reindex(anual.index.union(dias)).interpolate("time").ffill().bfill().reindex(dias)
    origem_cambio = "aproximado"
cambio_rel = cambio / cambio.mean()

# oferta local: talhões em colheita no dia, por cultura (histórico de safras do pipeline)
oferta = pd.DataFrame(0.0, index=dias, columns=list(BASE))
if spark.catalog.tableExists("campo_vivo.silver.safras"):
    saf = spark.table("campo_vivo.silver.safras").where("status = 'concluida'").select(
        "cultura", "data_inicio_colheita", "data_fim_colheita").toPandas()
    for _, s in saf.iterrows():
        oferta.loc[pd.Timestamp(s.data_inicio_colheita):pd.Timestamp(s.data_fim_colheita), s.cultura] += 1
oferta = oferta.rolling(21, min_periods=1, center=True).mean()
oferta_norm = (oferta - oferta.mean()) / oferta.std().replace(0, 1)

# forma mensal real da ComexStat por cultura × país (FOB/kg relativo à média), quando houver volume suficiente
forma = {}
if len(comex):
    for (cultura, pais_cfg) in {(c, p) for c in BASE for _, (_, p) in PRACAS.items() if p}:
        m = comex[(comex.cultura == cultura) & comex.pais.str.contains(pais_cfg, case=False, na=False)]
        m = m.groupby(["ano", "mes"])[["fob_usd", "kg"]].sum()
        m = m[m.kg >= 5000]
        if len(m) >= 12:
            preco = (m.fob_usd / m.kg)
            idx = pd.to_datetime([f"{a}-{mm:02d}-15" for a, mm in preco.index])
            s = pd.Series(preco.values / preco.mean(), index=idx)
            forma[(cultura, pais_cfg)] = s.reindex(s.index.union(dias)).interpolate("time").ffill().bfill().reindex(dias)

linhas = []
for cultura, base in BASE.items():
    mes_min, amp = SAZONAL[cultura]
    t = dias.dayofyear.values / 365.25 * 2 * np.pi
    saz = 1 + amp * np.cos(t - (mes_min - 0.5) / 12 * 2 * np.pi + np.pi)
    for praca, (fator, pais) in PRACAS.items():
        rng = np.random.default_rng(zlib.crc32(f"{cultura}|{praca}".encode()))
        eps = rng.normal(0, 0.018, len(dias))
        ar = np.zeros(len(dias))
        for i in range(1, len(dias)):
            ar[i] = 0.96 * ar[i - 1] + eps[i]
        nivel = base * fator * saz * (1 - 0.05 * oferta_norm[cultura].values)
        origem = "simulado"
        if pais:
            nivel = nivel * cambio_rel.values ** 0.6
            if (cultura, pais) in forma:
                nivel = base * fator * (1 - 0.03 * oferta_norm[cultura].values) * forma[(cultura, pais)].values * cambio_rel.values ** 0.6
                origem = "simulado_ancorado_comexstat"
        preco = np.round(nivel * np.exp(ar), 2)
        linhas += [(cultura, praca, d.date(), float(p), float(c), origem) for d, p, c in zip(dias, preco, cambio.values)]

serie = pd.DataFrame(linhas, columns=["cultura", "praca", "data", "preco_brl_kg", "usd_brl", "origem_preco"])
serie["origem_cambio"] = origem_cambio
salvar(com_metadados(spark.createDataFrame(serie), "mercado_simulado"), "bronze", "mercado_precos_diarios",
       comentario="Preço diário por cultura e praça (R$/kg), simulado; praças de exportação ancoradas na ComexStat quando disponível.")
print(serie.groupby(["cultura", "praca", "origem_preco"]).preco_brl_kg.agg(["mean", "min", "max"]).round(2))
