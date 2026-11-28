"""ML 03 · Mercado: previsão de preço por cultura × praça em 7, 14 e 30 dias + tabelas Gold da tela Mercado.

Alvo: retorno logarítmico do preço, log(preço[t+h] / preço[t]); preço previsto = preço[t] × exp(ŷ).
Prever o retorno (e não o preço) deixa um único modelo servir para todas as culturas e praças.
Variáveis: retornos e médias recentes do preço, volatilidade, sazonalidade (mês), câmbio e sua variação,
oferta do Vale nos próximos 30 dias (talhões com colheita prevista), cultura e praça.
Modelo: HistGradientBoostingRegressor com quantis p05/p50/p95 e calibração conformal do intervalo de 90%.
Validação temporal: últimos 180 dias como teste, comparando com duas regras simples —
  ingênua (o preço fica igual) e sazonal (repete a variação do mesmo período um ano antes).
"""
import sys
import datetime as dt
import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error
sys.path.insert(0, "/Volumes/campo_vivo/bronze/code")
from common import spark, salvar

HOJE = pd.Timestamp(dt.date.today())
HORIZONTES = [7, 14, 30]
CULTURAS = ["Goiaba", "Manga", "Melão", "Uva"]
PRACAS = ["CEAGESP (SP)", "Filadélfia (EUA)", "Londres (Reino Unido)", "Petrolina (PE)", "Roterdã (Países Baixos)"]
FEATS = ["cultura_cod", "praca_cod", "ret_7", "ret_30", "ret_90", "desvio_media_30", "volatilidade_30", "mes_sin",
         "mes_cos", "cambio_ret_30", "oferta_prox_30", "oferta_atual"]

p = spark.table("campo_vivo.silver.mercado_precos").toPandas()
p["data"] = pd.to_datetime(p["data"])
p = p.sort_values(["cultura", "praca", "data"])

# oferta do Vale: talhões em colheita (histórico real do pipeline + janelas previstas das safras atuais)
oferta = pd.DataFrame(0.0, index=pd.date_range(p.data.min(), HOJE + pd.Timedelta(days=60)), columns=CULTURAS)
saf = spark.table("campo_vivo.silver.safras").where("status = 'concluida'").select(
    "cultura", "data_inicio_colheita", "data_fim_colheita").toPandas()
janelas = [(r.cultura, r.data_inicio_colheita, r.data_fim_colheita) for r in saf.itertuples()]
if spark.catalog.tableExists("campo_vivo.gold.predicoes_talhao"):
    pr = spark.table("campo_vivo.gold.predicoes_talhao").select("cultura", "colheita_inicio", "colheita_fim").toPandas()
    janelas += [(r.cultura, r.colheita_inicio, r.colheita_fim) for r in pr.itertuples()]
for c, i, f in janelas:
    oferta.loc[pd.Timestamp(i):pd.Timestamp(f), c] += 1
prox30 = oferta[::-1].rolling(30, min_periods=1).mean()[::-1]  # média dos próximos 30 dias

linhas = []
for (cultura, praca), g in p.groupby(["cultura", "praca"]):
    s = g.set_index("data").preco_brl_kg.asfreq("D").ffill()
    cb = g.set_index("data").usd_brl.asfreq("D").ffill()
    lp = np.log(s)
    df = pd.DataFrame({"data": s.index, "preco": s.values})
    df["cultura"], df["praca"] = cultura, praca
    for k in (7, 30, 90):
        df[f"ret_{k}"] = (lp - lp.shift(k)).values
    df["desvio_media_30"] = (lp - np.log(s.rolling(30).mean())).values
    df["volatilidade_30"] = lp.diff().rolling(30).std().values
    df["cambio_ret_30"] = np.log(cb / cb.shift(30)).values
    df["mes_sin"] = np.sin(2 * np.pi * df.data.dt.dayofyear / 365.25)
    df["mes_cos"] = np.cos(2 * np.pi * df.data.dt.dayofyear / 365.25)
    df["oferta_prox_30"] = prox30[cultura].reindex(df.data).values
    df["oferta_atual"] = oferta[cultura].reindex(df.data).values
    for h in HORIZONTES:
        df[f"y_{h}"] = (lp.shift(-h) - lp).values
        df[f"y_saz_{h}"] = (lp.shift(365 - h) - lp.shift(365)).values  # mesma variação um ano antes
    linhas.append(df)
D = pd.concat(linhas, ignore_index=True)
D["cultura_cod"] = D.cultura.map({c: i for i, c in enumerate(CULTURAS)}).astype(float)
D["praca_cod"] = D.praca.map({c: i for i, c in enumerate(PRACAS)}).astype(float)
D = D[D.data >= D.data.min() + pd.Timedelta(days=95)]
cat = [f in ("cultura_cod", "praca_cod") for f in FEATS]
CORTE = HOJE - pd.Timedelta(days=180)


def modelo(q=None):
    kw = dict(max_iter=300, learning_rate=0.05, max_leaf_nodes=31, min_samples_leaf=40, l2_regularization=1.0,
              categorical_features=cat, random_state=7)
    return HistGradientBoostingRegressor(loss="quantile", quantile=q, **kw) if q else HistGradientBoostingRegressor(**kw)


def mape(y_true_ret, y_pred_ret):
    return float(np.mean(np.abs(np.exp(y_pred_ret - y_true_ret) - 1)) * 100)


mlflow.set_experiment("/Shared/campo_vivo_mercado")
metricas, finais, ajuste = {}, {}, {}
with mlflow.start_run(run_name=f"mercado_{HOJE.date()}") as run:
    mlflow.log_params({"modelo": "HistGradientBoostingRegressor", "alvo": "log(preco[t+h]/preco[t])",
                       "horizontes": str(HORIZONTES), "teste_desde": str(CORTE.date()), "features": ",".join(FEATS)})
    for h in HORIZONTES:
        y = f"y_{h}"
        A = D.dropna(subset=FEATS[2:6] + [y])
        tr = A[A.data + pd.Timedelta(days=h) < CORTE]
        te = A[(A.data >= CORTE) & (A.data + pd.Timedelta(days=h) <= HOJE)]
        m = modelo().fit(tr[FEATS], tr[y])
        lo, hi = modelo(0.05).fit(tr[FEATS], tr[y]), modelo(0.95).fit(tr[FEATS], tr[y])
        pr, plo, phi = m.predict(te[FEATS]), lo.predict(te[FEATS]), hi.predict(te[FEATS])
        E = np.maximum(plo - te[y].values, te[y].values - phi)
        Q = float(np.quantile(E, min(1.0, 0.9 * (1 + 1 / len(E)))))
        ajuste[h] = Q
        reais = te.preco.values * np.exp(te[y].values)
        saz = te[f"y_saz_{h}"].fillna(0).values
        r = dict(
            mape=mape(te[y].values, pr), mape_ingenuo=mape(te[y].values, np.zeros(len(te))),
            mape_sazonal=mape(te[y].values, saz),
            mae_brl=mean_absolute_error(reais, te.preco.values * np.exp(pr)),
            mae_brl_ingenuo=mean_absolute_error(reais, te.preco.values),
            acerto_direcao=float(np.mean(np.sign(pr) == np.sign(te[y].values))),
            cobertura_90=float(np.mean((te[y].values >= plo - Q) & (te[y].values <= phi + Q))),
            n_treino=len(tr), n_teste=len(te))
        metricas[h] = r
        mlflow.log_metrics({f"h{h}__{k}": float(v) for k, v in r.items()})
        Aall = A[A.data + pd.Timedelta(days=h) <= HOJE]
        finais[h] = {q: (modelo(q) if q else modelo()).fit(Aall[FEATS], Aall[y]) for q in (None, 0.05, 0.95)}
        mlflow.sklearn.log_model(finais[h][None], name=f"modelo_preco_h{h}", input_example=Aall[FEATS].head(3))
        print(f"h={h}: MAPE {r['mape']:.2f}% · ingênuo {r['mape_ingenuo']:.2f}% · sazonal {r['mape_sazonal']:.2f}% · "
              f"direção {r['acerto_direcao']:.0%} · cobertura {r['cobertura_90']:.0%}")
    run_id = run.info.run_id

# ---------------- previsões a partir do último dia ----------------
ult = D[D.data == D.data.max()].copy()
prev = []
for h in HORIZONTES:
    X = ult[FEATS]
    p50 = finais[h][None].predict(X)
    p05 = np.minimum(finais[h][0.05].predict(X) - ajuste[h], p50)
    p95 = np.maximum(finais[h][0.95].predict(X) + ajuste[h], p50)
    for (_, r), a, b, c in zip(ult.iterrows(), p50, p05, p95):
        prev.append(dict(cultura=r.cultura, praca=r.praca, horizonte_dias=h, data_base=r.data.date(),
                         data_alvo=(r.data + pd.Timedelta(days=h)).date(), preco_atual=round(r.preco, 2),
                         preco_previsto=round(r.preco * np.exp(a), 2), preco_p05=round(r.preco * np.exp(b), 2),
                         preco_p95=round(r.preco * np.exp(c), 2), variacao_prevista_pct=round((np.exp(a) - 1) * 100, 1)))
P = pd.DataFrame(prev)
P["modelo_run_id"], P["gerado_em"] = run_id, pd.Timestamp.utcnow().isoformat()

# ---------------- referências por praça (tabela da tela) ----------------
fr = spark.table("campo_vivo.silver.mercado_fretes").toPandas().set_index("praca").frete_brl_kg
ref = []
for (cultura, praca), g in p.groupby(["cultura", "praca"]):
    g = g.set_index("data").preco_brl_kg
    atual = g.iloc[-1]
    j90 = g[g.index > g.index.max() - pd.Timedelta(days=90)]
    p30 = P[(P.cultura == cultura) & (P.praca == praca) & (P.horizonte_dias == 30)].iloc[0]
    frete = float(fr.get(praca, 0.0))
    ref.append(dict(cultura=cultura, praca=praca, data=g.index.max().date(), preco_atual=round(atual, 2),
                    variacao_30d_pct=round((atual / g.iloc[-31] - 1) * 100, 1),
                    variacao_90d_pct=round((atual / j90.iloc[0] - 1) * 100, 1),
                    minima_90d=round(j90.min(), 2), maxima_90d=round(j90.max(), 2), frete_brl_kg=frete,
                    liquido_brl_kg=round(atual - frete, 2), preco_previsto_30d=p30.preco_previsto,
                    liquido_previsto_30d=round(p30.preco_previsto - frete, 2)))
R = pd.DataFrame(ref)
R["melhor_liquido"] = R.groupby("cultura").liquido_brl_kg.transform("max") == R.liquido_brl_kg
R["melhor_liquido_previsto_30d"] = R.groupby("cultura").liquido_previsto_30d.transform("max") == R.liquido_previsto_30d
R["origem_preco"] = R.merge(p.groupby(["cultura", "praca"]).origem_preco.last().reset_index(), on=["cultura", "praca"]).origem_preco

serie = p[p.data > HOJE - pd.Timedelta(days=365)][["cultura", "praca", "data", "preco_brl_kg", "origem_preco"]].copy()
serie["data"] = serie.data.dt.date
salvar(spark.createDataFrame(serie), "gold", "mercado_serie",
       comentario="Preço diário por cultura e praça, últimos 365 dias (gráfico da tela Mercado).")
salvar(spark.createDataFrame(R), "gold", "mercado_referencias",
       comentario="Referências por praça: preço, variação, mínima/máxima 90 dias, frete, líquido e melhor destino.")
salvar(spark.createDataFrame(P), "gold", "mercado_previsao",
       comentario="Previsão de preço por cultura e praça em 7, 14 e 30 dias, com intervalo de 90%.")
meta = pd.DataFrame([dict(modelo=f"preco_h{h}", horizonte_dias=h, **{k: (round(v, 4) if isinstance(v, float) else v) for k, v in m.items()},
                          mlflow_run_id=run_id, treinado_em=pd.Timestamp.utcnow().isoformat()) for h, m in metricas.items()])
salvar(spark.createDataFrame(meta), "gold", "mercado_modelo_metadados",
       comentario="Métricas da validação temporal (180 dias) do modelo de preço e comparação com regras simples.")
print(R[R.cultura == "Manga"][["praca", "preco_atual", "variacao_30d_pct", "frete_brl_kg", "liquido_brl_kg",
                                "preco_previsto_30d", "melhor_liquido", "melhor_liquido_previsto_30d"]].to_string())
