# Databricks notebook source
# MAGIC %md
# MAGIC # Campo Vivo · Análise Exploratória dos Dados Climáticos (item 4)
# MAGIC
# MAGIC Fonte: camadas **Silver** e **Gold** do catálogo `campo_vivo` (leituras IoT tratadas, clima de referência Open-Meteo,
# MAGIC safras e vigor). Linguagem: Python (R não é suportado na Databricks Free Edition).
# MAGIC
# MAGIC Perguntas:
# MAGIC 1. Qual a temperatura média, máxima e mínima por fazenda, cultura e talhão?
# MAGIC 2. Como a umidade (ar e solo) varia ao longo do dia e do ano?
# MAGIC 3. Qual o comportamento climático por período (mês, estação seca × chuvosa, ano)?
# MAGIC 4. Como as propriedades, culturas e lotes se comparam — e quanto tempo passam fora da faixa ideal?
# MAGIC 5. Qual a relação entre clima, vigor e produtividade (base para as regressões)?
# MAGIC 6. Qualidade dos dados: o que a Silver tratou?
# MAGIC
# MAGIC > ⚠️ Leituras IoT, safras e vigor são **simulados** a partir do clima real do Open-Meteo (ver README do repositório).

# COMMAND ----------

import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()
SAIDA = "/Volumes/campo_vivo/gold/relatorios/eda"
os.makedirs(SAIDA, exist_ok=True)
plt.rcParams.update({"figure.figsize": (11, 4.5), "axes.grid": True, "grid.alpha": 0.3, "axes.spines.top": False,
                     "axes.spines.right": False, "font.size": 10})
CORES = {"Manga": "#E8A33D", "Uva": "#8E5BB5", "Goiaba": "#D9534F", "Melão": "#9DBF3B"}
FAZ = {r.fazenda_id: r.nome for r in spark.table("campo_vivo.silver.fazendas").collect()}


def salvar_fig(nome):
    plt.tight_layout()
    plt.savefig(f"{SAIDA}/{nome}.png", dpi=110)
    plt.show()
    plt.close()


def mostrar(df, n=30):
    try:
        display(df)  # noqa: F821 — disponível no notebook
    except NameError:
        print(df.head(n).to_string() if isinstance(df, pd.DataFrame) else df.limit(n).toPandas().to_string())


diario = (spark.table("campo_vivo.gold.clima_diario_talhao")
          .join(spark.table("campo_vivo.silver.talhoes").select("talhao_codigo", "nome", "variedade"), "talhao_codigo")
          .toPandas())
diario["data"] = pd.to_datetime(diario["data"])
diario["fazenda"] = diario["fazenda_id"].map(FAZ)
diario["mes"] = diario["data"].dt.month
diario["ano"] = diario["data"].dt.year
diario["periodo"] = diario["mes"].map(lambda m: "chuvoso (jan–abr)" if m in (1, 2, 3, 4) else "seco (mai–dez)")
print(f"{len(diario):,} dias × talhão · {diario.talhao_codigo.nunique()} talhões · {diario.data.min().date()} → {diario.data.max().date()}")

# COMMAND ----------

# MAGIC %md ## 1. Temperatura média, máxima e mínima

# COMMAND ----------

t_fazenda = (diario.groupby("fazenda")
             .agg(temp_media=("temp_media", "mean"), temp_max_media=("temp_max", "mean"), temp_min_media=("temp_min", "mean"),
                  recorde_max=("temp_max", "max"), recorde_min=("temp_min", "min"),
                  amplitude_media=("temp_max", lambda s: (s - diario.loc[s.index, "temp_min"]).mean()))
             .round(1))
mostrar(t_fazenda.reset_index())

t_cultura = (diario.groupby("cultura")
             .agg(temp_media=("temp_media", "mean"), temp_max=("temp_max", "max"), temp_min=("temp_min", "min"),
                  horas_acima_faixa_dia=("horas_acima_tmax", "mean"))
             .round(1))
mostrar(t_cultura.reset_index())

fig, ax = plt.subplots()
for faz, g in diario.groupby("fazenda"):
    m = g.groupby(g.data.dt.to_period("M")).temp_media.mean()
    ax.plot(m.index.to_timestamp(), m.values, label=faz, lw=1.6)
ax.set_title("Temperatura média mensal por fazenda (°C)")
ax.legend()
salvar_fig("01_temperatura_mensal_fazenda")

# COMMAND ----------

# MAGIC %md ## 2. Variação da umidade (ar e solo)

# COMMAND ----------

h = (spark.table("campo_vivo.silver.leituras_horarias")
     .withColumn("hora_local", F.hour(F.col("hora_utc") - F.expr("INTERVAL 3 HOURS")))
     .withColumn("mes", F.month(F.col("hora_utc") - F.expr("INTERVAL 3 HOURS")))
     .groupBy("fazenda_id", "hora_local").agg(F.avg("umidade").alias("umidade"), F.avg("temperatura").alias("temperatura"),
                                             F.avg("umidade_solo").alias("umidade_solo"))
     .toPandas())
h["fazenda"] = h["fazenda_id"].map(FAZ)
fig, axs = plt.subplots(1, 2)
for faz, g in h.sort_values("hora_local").groupby("fazenda"):
    axs[0].plot(g.hora_local, g.umidade, label=faz)
    axs[1].plot(g.hora_local, g.temperatura, label=faz)
axs[0].set_title("Ciclo diário da umidade do ar (%)")
axs[1].set_title("Ciclo diário da temperatura (°C)")
for a in axs:
    a.set_xlabel("hora local")
axs[0].legend()
salvar_fig("02_ciclo_diario_umidade_temperatura")

u = (diario.groupby(["fazenda", "mes"]).agg(umidade_ar=("umidade_media", "mean"), umidade_solo=("umidade_solo_media", "mean"),
                                            variacao_umidade_dia=("umidade_max", lambda s: (s - diario.loc[s.index, "umidade_min"]).mean()))
     .round(1).reset_index())
mostrar(u)
fig, axs = plt.subplots(1, 2)
for faz, g in u.groupby("fazenda"):
    axs[0].plot(g.mes, g.umidade_ar, marker="o", label=faz)
    axs[1].plot(g.mes, g.umidade_solo, marker="o", label=faz)
axs[0].set_title("Umidade do ar média por mês (%)")
axs[1].set_title("Umidade do solo média por mês (%)")
axs[0].legend()
salvar_fig("03_umidade_por_mes")

# COMMAND ----------

# MAGIC %md ## 3. Comportamento climático por período

# COMMAND ----------

per = (diario.groupby(["fazenda", "periodo"])
       .agg(temp_media=("temp_media", "mean"), umidade=("umidade_media", "mean"), chuva_mm_dia=("chuva_mm", "mean"),
            horas_condicao_adequada_dia=("horas_condicao_adequada", "mean"))
       .round(2).reset_index())
mostrar(per)
anual = (diario.groupby(["fazenda", "ano"]).agg(chuva_total_mm=("chuva_mm", lambda s: s.sum() / diario.loc[s.index, "talhao_codigo"].nunique()),
                                                temp_media=("temp_media", "mean")).round(1).reset_index())
mostrar(anual)
fig, ax = plt.subplots()
for faz, g in anual.groupby("fazenda"):
    ax.bar(g.ano + list(FAZ.values()).index(faz) * 0.27 - 0.27, g.chuva_total_mm, width=0.27, label=faz)
ax.set_title("Chuva anual por fazenda (mm) — 2026 parcial")
ax.legend()
salvar_fig("04_chuva_anual")

# COMMAND ----------

# MAGIC %md ## 4. Comparação entre propriedades, culturas e lotes — tempo fora da faixa ideal

# COMMAND ----------

lote = (diario[diario.data >= diario.data.max() - pd.Timedelta(days=365)]
        .groupby(["fazenda", "talhao_codigo", "cultura"])
        .agg(temp_media=("temp_media", "mean"), umidade_solo=("umidade_solo_media", "mean"),
             pct_horas_temp_fora=("horas_temp_fora_faixa", "sum"), pct_horas_umid_fora=("horas_umid_fora_faixa", "sum"),
             horas=("horas_com_leitura", "sum"), completude_pct=("completude_pct", "mean"))
        .reset_index())
lote["pct_horas_temp_fora"] = (lote.pct_horas_temp_fora / lote.horas * 100).round(1)
lote["pct_horas_umid_fora"] = (lote.pct_horas_umid_fora / lote.horas * 100).round(1)
lote = lote.drop(columns="horas").round(1).sort_values("pct_horas_temp_fora", ascending=False)
mostrar(lote)

fig, ax = plt.subplots(figsize=(11, 5))
ax.barh(lote.talhao_codigo, lote.pct_horas_temp_fora, color=[CORES[c] for c in lote.cultura])
ax.invert_yaxis()
ax.set_title("% das horas com temperatura fora da faixa ideal da cultura (últimos 12 meses)")
for c, cor in CORES.items():
    ax.barh([], [], color=cor, label=c)
ax.legend()
salvar_fig("05_horas_fora_faixa_por_lote")

fig, ax = plt.subplots()
dados = [diario[diario.cultura == c].temp_media.dropna() for c in CORES]
ax.boxplot(dados, labels=list(CORES), showfliers=False)
ax.set_title("Distribuição da temperatura média diária por cultura (°C)")
salvar_fig("06_temperatura_por_cultura")

# COMMAND ----------

# MAGIC %md ## 5. Clima × vigor × produtividade (base das regressões)

# COMMAND ----------

f = spark.table("campo_vivo.gold.features_safra").where("conjunto = 'treino'").toPandas()
f = f.sort_values("fracao_ciclo").groupby("safra_id").tail(1)  # 1 linha por safra (ponto mais tardio)
X = ["umidade_solo_media", "temp_media", "frac_horas_acima_tmax", "chuva_prevista_14d", "produtividade_safra_anterior",
     "vigor_max", "produtividade_t_ha"]
corr = f[X].astype(float).corr().round(2)
mostrar(corr.reset_index())
fig, axs = plt.subplots(1, 3, figsize=(13, 4))
for a, col, tit in zip(axs, ["vigor_max", "frac_horas_acima_tmax", "produtividade_safra_anterior"],
                       ["Vigor máximo", "Fração de horas acima da faixa", "Produtividade da safra anterior"]):
    for c, g in f.groupby("cultura"):
        a.scatter(g[col], g.produtividade_t_ha, s=14, alpha=0.7, color=CORES[c], label=c)
    a.set_title(f"{tit} × produtividade")
axs[0].legend()
salvar_fig("07_clima_vigor_produtividade")

# COMMAND ----------

# MAGIC %md ## 6. Qualidade dos dados tratada na Silver

# COMMAND ----------

q = spark.table("campo_vivo.silver.qualidade_leituras").orderBy(F.desc("count")).toPandas()
mostrar(q)
comp = (diario.groupby("fazenda").completude_pct.mean().round(1))
print("Completude média das leituras horárias (%):", comp.to_dict())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusões
# MAGIC Os números abaixo são recalculados a cada execução e impressos na última célula; a leitura esperada é:
# MAGIC * **Petrolina e Juazeiro** têm clima quase idêntico (semiárido, ~500–550 mm/ano, chuva concentrada de jan a abr);
# MAGIC   **Mossoró** é mais úmida no ar e chove mais.
# MAGIC * A **umidade do ar** cai cerca de 30–40 pontos entre a madrugada e as 14–15 h; a do **solo** é sustentada pela irrigação
# MAGIC   e cai nos dias mais quentes.
# MAGIC * **Uva e manga** passam a maior parte das horas de sol acima do teto da faixa (30 °C) — é o principal estresse.
# MAGIC * Para produtividade, **vigor** e **histórico do talhão** são as variáveis mais correlacionadas; o calor age
# MAGIC   principalmente através do vigor — o que explica os pesos do modelo.

# COMMAND ----------

resumo = {
    "temp_media_por_fazenda": t_fazenda.temp_media.to_dict(),
    "pct_horas_temp_fora_por_cultura": lote.groupby("cultura").pct_horas_temp_fora.mean().round(1).to_dict(),
    "correlacao_com_produtividade": corr["produtividade_t_ha"].drop("produtividade_t_ha").to_dict(),
}
print(resumo)
pd.Series({k: str(v) for k, v in resumo.items()}).to_json(f"{SAIDA}/resumo.json", force_ascii=False)
