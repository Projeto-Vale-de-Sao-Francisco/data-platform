"""Gold 02 · Tabela de features para as regressões (treino + linhas a prever).

Uma linha por talhão × safra × ponto de referência. Para safras concluídas, pontos em 50%…90% do
ciclo nominal (só antes do início real da colheita); para safras em andamento, o ponto é hoje.

X (as 5 variáveis do projeto + cultura):
  umidade do solo · temperatura · chuva prevista (14 dias à frente) · histórico do talhão · índice de vigor · cultura
y:
  produtividade_t_ha · dias_ate_inicio_colheita · duracao_colheita · offset_exportacao · duracao_exportacao

"Chuva prevista" em linhas históricas = chuva observada nos 14 dias seguintes (proxy de previsão
perfeita); nas linhas de hoje = previsão Open-Meteo.
"""
import sys, datetime as dt
import numpy as np
import pandas as pd
sys.path.insert(0, "/Volumes/campo_vivo/bronze/code")
from common import spark, salvar

CICLO = {"Manga": 145, "Uva": 110, "Goiaba": 180, "Melão": 70}
HOJE = pd.Timestamp(dt.date.today())

d = spark.table("campo_vivo.gold.clima_diario_talhao").select(
    "talhao_codigo", "fazenda_id", "data", "temp_media", "temp_max", "umidade_media", "umidade_solo_media",
    "chuva_mm", "chuva_ref", "graus_dia", "horas_acima_tmax", "horas_com_leitura", "horas_condicao_adequada").toPandas()
d["data"] = pd.to_datetime(d["data"])
d["chuva"] = d["chuva_mm"].fillna(d["chuva_ref"])
vig = spark.table("campo_vivo.gold.vigor_diario").toPandas()
vig["data"] = pd.to_datetime(vig["data"])
saf = spark.table("campo_vivo.silver.safras").toPandas()
for c in ["data_plantio", "data_inicio_colheita", "data_fim_colheita", "data_inicio_exportacao", "data_fim_exportacao"]:
    saf[c] = pd.to_datetime(saf[c])
prev = spark.table("campo_vivo.silver.previsao_diaria").toPandas()
prev["data"] = pd.to_datetime(prev["data"])
tal = spark.table("campo_vivo.silver.talhoes").select("talhao_codigo", "fazenda_id").toPandas()

linhas = []
for _, s in saf.sort_values(["talhao_codigo", "data_plantio"]).iterrows():
    ciclo = CICLO[s.cultura]
    dd = d[d.talhao_codigo == s.talhao_codigo].set_index("data").sort_index()
    vv = vig[vig.talhao_codigo == s.talhao_codigo].set_index("data").sort_index()
    anteriores = saf[(saf.talhao_codigo == s.talhao_codigo) & (saf.data_plantio < s.data_plantio) & (saf.status == "concluida")]
    fazenda = int(tal[tal.talhao_codigo == s.talhao_codigo].fazenda_id.iloc[0])
    if s.status == "concluida":
        refs = [s.data_plantio + pd.Timedelta(days=int(f * ciclo)) for f in (0.5, 0.6, 0.7, 0.8, 0.9)]
        refs = [r for r in refs if r < s.data_inicio_colheita]
    else:
        refs = [HOJE]
    for ref in refs:
        jan = dd[s.data_plantio:ref]
        if len(jan) < 10:
            continue
        ult30 = jan[jan.index > ref - pd.Timedelta(days=30)]
        if s.status == "concluida":
            chuva_prev = dd[ref + pd.Timedelta(days=1):ref + pd.Timedelta(days=14)].chuva.sum()
        else:
            p = prev[(prev.fazenda_id == fazenda) & (prev.data > ref) & (prev.data <= ref + pd.Timedelta(days=14))]
            chuva_prev = p.chuva_prevista_mm.sum()
        vj = vv[s.data_plantio:ref].vigor.dropna()
        v30 = vj[vj.index > ref - pd.Timedelta(days=30)]
        inclin = np.polyfit((v30.index - ref).days, v30.values, 1)[0] * 30 if len(v30) >= 5 else np.nan
        linhas.append(dict(
            talhao_codigo=s.talhao_codigo, safra_id=s.safra_id, cultura=s.cultura, variedade=s.variedade,
            status=s.status, data_referencia=ref.date(), data_plantio=s.data_plantio.date(),
            dias_desde_plantio=(ref - s.data_plantio).days, fracao_ciclo=round((ref - s.data_plantio).days / ciclo, 3),
            # X1 umidade do solo
            umidade_solo_media=jan.umidade_solo_media.mean(), umidade_solo_30d=ult30.umidade_solo_media.mean(),
            # X2 temperatura
            temp_media=jan.temp_media.mean(), temp_media_30d=ult30.temp_media.mean(),
            graus_dia_acum=jan.graus_dia.sum(),
            frac_horas_acima_tmax=jan.horas_acima_tmax.sum() / max(jan.horas_com_leitura.sum(), 1),
            # X3 chuva prevista
            chuva_prevista_14d=float(chuva_prev), chuva_acum_30d=ult30.chuva.sum(),
            # X4 histórico do talhão
            produtividade_safra_anterior=anteriores.produtividade_t_ha.iloc[-1] if len(anteriores) else np.nan,
            produtividade_media_talhao=anteriores.produtividade_t_ha.mean() if len(anteriores) else np.nan,
            dias_colheita_safra_anterior=anteriores.dias_ate_colheita.iloc[-1] if len(anteriores) else np.nan,
            # X5 índice de vigor
            vigor_atual=vj.iloc[-1] if len(vj) else np.nan, vigor_max=vj.max() if len(vj) else np.nan,
            vigor_tendencia_30d=inclin,
            # y
            produtividade_t_ha=s.produtividade_t_ha,
            dias_ate_inicio_colheita=(s.data_inicio_colheita - ref).days if s.status == "concluida" else np.nan,
            duracao_colheita=s.duracao_colheita,
            offset_exportacao=(s.data_inicio_exportacao - s.data_inicio_colheita).days if s.status == "concluida" else np.nan,
            duracao_exportacao=(s.data_fim_exportacao - s.data_inicio_exportacao).days if s.status == "concluida" else np.nan,
            destino_exportacao=s.destino_exportacao,
        ))

f = pd.DataFrame(linhas)
num = f.select_dtypes("number").columns
f[num] = f[num].astype(float).round(4)
f["conjunto"] = np.where(f.status == "concluida", "treino", "prever")
salvar(spark.createDataFrame(f.astype(object).where(f.notna(), None)), "gold", "features_safra",
       comentario="Features por talhão × safra × ponto de referência para as regressões (treino e previsão).")
print(f.groupby(["conjunto", "cultura"]).size())
print(f[f.conjunto == "treino"][["umidade_solo_media", "temp_media", "chuva_prevista_14d", "produtividade_safra_anterior",
                                  "vigor_max", "produtividade_t_ha"]].corr().round(2))
