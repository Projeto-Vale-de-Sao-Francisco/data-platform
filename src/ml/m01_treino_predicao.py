"""ML 01 · Regressões (produtividade, janela de colheita, janela de exportação, curva de vigor) + predições.

Modelo: HistGradientBoostingRegressor (aceita nulos, captura não linearidade) com perda quantílica
para p05 / p50 / p95. Validação temporal: treino com safras plantadas antes de 2025-06-01,
teste com as posteriores. Depois da avaliação, os modelos finais são reajustados com tudo.
Tudo é registrado no MLflow; as predições vão para tabelas Gold consumidas pela aplicação.
"""
import sys, datetime as dt, json
import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, r2_score
sys.path.insert(0, "/Volumes/campo_vivo/bronze/code")
from common import spark, salvar

HOJE = pd.Timestamp(dt.date.today())
CORTE = pd.Timestamp("2025-06-01")
GRUPOS = {
    "Umidade do solo": ["umidade_solo_media", "umidade_solo_30d"],
    "Temperatura": ["temp_media", "temp_media_30d", "graus_dia_acum", "frac_horas_acima_tmax"],
    "Chuva prevista": ["chuva_prevista_14d", "chuva_acum_30d"],
    "Histórico do talhão": ["produtividade_safra_anterior", "produtividade_media_talhao", "dias_colheita_safra_anterior"],
    "Índice de vigor": ["vigor_atual", "vigor_max", "vigor_tendencia_30d"],
    "Cultura": ["cultura_cod"],
    "Estágio do ciclo": ["dias_desde_plantio", "fracao_ciclo"],
}
FEATS = [c for g in GRUPOS.values() for c in g]
CULTURAS = ["Goiaba", "Manga", "Melão", "Uva"]

f = spark.table("campo_vivo.gold.features_safra").toPandas()
f["cultura_cod"] = f["cultura"].map({c: i for i, c in enumerate(CULTURAS)}).astype(float)
for c in ["data_referencia", "data_plantio"]:
    f[c] = pd.to_datetime(f[c])
num = [c for c in FEATS + ["produtividade_t_ha", "dias_ate_inicio_colheita", "duracao_colheita", "offset_exportacao",
                           "duracao_exportacao"] if c in f]
f[num] = f[num].astype(float)
treino_all = f[f.conjunto == "treino"].copy()
prever = f[f.conjunto == "prever"].copy()
tr = treino_all[treino_all.data_plantio < CORTE]
te = treino_all[treino_all.data_plantio >= CORTE]
cat_mask = [c == "cultura_cod" for c in FEATS]


def modelo(q=None):
    kw = dict(max_iter=300, learning_rate=0.05, max_leaf_nodes=15, min_samples_leaf=15, l2_regularization=1.0,
              categorical_features=cat_mask, random_state=42)
    return HistGradientBoostingRegressor(loss="quantile", quantile=q, **kw) if q else HistGradientBoostingRegressor(**kw)


def importancia_agrupada(m, X, y, rep=20, seed=0):
    rng = np.random.default_rng(seed)
    base = mean_absolute_error(y, m.predict(X))
    imp = {}
    for g, cols in GRUPOS.items():
        perdas = []
        for _ in range(rep):
            Xp = X.copy()
            idx = rng.permutation(len(Xp))
            Xp[cols] = Xp[cols].values[idx]
            perdas.append(mean_absolute_error(y, m.predict(Xp)) - base)
        imp[g] = max(np.mean(perdas), 0)
    tot = sum(imp.values()) or 1
    return {g: v / tot for g, v in imp.items()}


ALVOS = {
    "produtividade_t_ha": "Produtividade (t/ha)",
    "dias_ate_inicio_colheita": "Dias até o início da colheita",
    "duracao_colheita": "Duração da colheita (dias)",
    "offset_exportacao": "Dias entre início da colheita e início da exportação",
    "duracao_exportacao": "Duração da janela de exportação (dias)",
}
mlflow.set_experiment("/Shared/campo_vivo_predicoes")
metricas, finais, fatores, ajuste, estrategia, medias = {}, {}, {}, {}, {}, {}
with mlflow.start_run(run_name=f"treino_{HOJE.date()}") as run:
    mlflow.log_params({"modelo": "HistGradientBoostingRegressor", "corte_temporal": str(CORTE.date()),
                       "n_treino": len(tr), "n_teste": len(te), "n_safras": treino_all.safra_id.nunique(),
                       "features": ",".join(FEATS)})
    for alvo in ALVOS:
        a_tr, a_te = tr.dropna(subset=[alvo]), te.dropna(subset=[alvo])
        m = modelo().fit(a_tr[FEATS], a_tr[alvo])
        lo, hi = modelo(0.05).fit(a_tr[FEATS], a_tr[alvo]), modelo(0.95).fit(a_tr[FEATS], a_tr[alvo])
        p = m.predict(a_te[FEATS])
        p_lo, p_hi = lo.predict(a_te[FEATS]), hi.predict(a_te[FEATS])
        ingenuo = a_tr.groupby("cultura")[alvo].mean()  # baseline: média da cultura
        mae_base = mean_absolute_error(a_te[alvo], a_te.cultura.map(ingenuo))
        # calibração conformal (CQR): alarga o intervalo quantílico até cobrir 90% no teste temporal
        E = np.maximum(p_lo - a_te[alvo].values, a_te[alvo].values - p_hi)
        Q = float(np.quantile(E, min(1.0, 0.9 * (1 + 1 / len(E)))))
        ajuste[alvo] = Q
        estrategia[alvo] = "ml" if mean_absolute_error(a_te[alvo], p) < mae_base else "media_por_cultura"
        r = dict(mae=mean_absolute_error(a_te[alvo], p), r2=r2_score(a_te[alvo], p), mae_baseline=mae_base,
                 cobertura_90_bruta=float(np.mean((a_te[alvo] >= p_lo) & (a_te[alvo] <= p_hi))),
                 cobertura_90=float(np.mean((a_te[alvo] >= p_lo - Q) & (a_te[alvo] <= p_hi + Q))))
        metricas[alvo] = r
        mlflow.log_metrics({f"{alvo}__{k}": float(v) for k, v in r.items()})
        if alvo == "produtividade_t_ha":
            fatores = importancia_agrupada(m, a_te[FEATS].reset_index(drop=True), a_te[alvo].reset_index(drop=True))
            mlflow.log_dict(fatores, "fatores_produtividade.json")
        # modelos finais com todo o histórico
        a_all = treino_all.dropna(subset=[alvo])
        finais[alvo] = {q: (modelo(q) if q else modelo()).fit(a_all[FEATS], a_all[alvo]) for q in (None, 0.05, 0.95)}
        medias[alvo] = a_all.groupby("cultura")[alvo].mean()
        mlflow.sklearn.log_model(finais[alvo][None], name=f"modelo_{alvo}", input_example=a_all[FEATS].head(3))
    mlflow.log_dict({"estrategia": estrategia, "ajuste_conformal": ajuste}, "estrategia.json")
    print(json.dumps(metricas, indent=1, ensure_ascii=False))

    # ---------- curva de vigor: vigor daqui a h dias (h = 7…63) ----------
    vig = spark.table("campo_vivo.gold.vigor_diario").toPandas()
    vig["data"] = pd.to_datetime(vig["data"])
    vig = vig.dropna(subset=["vigor"]).set_index(["talhao_codigo", "data"]).vigor
    saf = spark.table("campo_vivo.silver.safras").toPandas()
    saf["data_plantio"] = pd.to_datetime(saf["data_plantio"])
    saf["data_fim_colheita"] = pd.to_datetime(saf["data_fim_colheita"])
    CICLO = {"Manga": 145, "Uva": 110, "Goiaba": 180, "Melão": 70}
    lin = []
    for _, s in saf[saf.status == "concluida"].iterrows():
        try:
            serie = vig.loc[s.talhao_codigo]
        except KeyError:
            continue
        serie = serie[s.data_plantio:s.data_fim_colheita]
        for ref in serie.index[10::10]:
            for hz in range(7, 64, 7):
                alvo_d = ref + pd.Timedelta(days=hz)
                if alvo_d in serie.index:
                    lin.append(dict(cultura_cod=CULTURAS.index(s.cultura), fracao=(ref - s.data_plantio).days / CICLO[s.cultura],
                                    h=hz, vigor_ref=serie[ref], vigor_ref_7=serie.get(ref - pd.Timedelta(days=7), np.nan),
                                    y=serie[alvo_d], plantio=s.data_plantio))
    V = pd.DataFrame(lin)
    VF = ["cultura_cod", "fracao", "h", "vigor_ref", "vigor_ref_7"]
    kwv = dict(max_iter=250, learning_rate=0.05, categorical_features=[True, False, False, False, False], random_state=1)
    vtr, vte = V[V.plantio < CORTE], V[V.plantio >= CORTE]
    mv = HistGradientBoostingRegressor(**kwv).fit(vtr[VF], vtr.y)
    metricas["vigor_curva"] = dict(mae=mean_absolute_error(vte.y, mv.predict(vte[VF])), r2=r2_score(vte.y, mv.predict(vte[VF])))
    mlflow.log_metrics({"vigor__mae": metricas["vigor_curva"]["mae"], "vigor__r2": metricas["vigor_curva"]["r2"]})
    vmod = {q: HistGradientBoostingRegressor(**({**kwv, "loss": "quantile", "quantile": q} if q else kwv)).fit(V[VF], V.y)
            for q in (None, 0.05, 0.95)}
    run_id = run.info.run_id

# ---------- predições para as safras em andamento ----------
P = prever.copy()
X = P[FEATS]
pred = {}
for a in ALVOS:
    if estrategia[a] == "ml":
        pred[a] = {None: finais[a][None].predict(X), 0.05: finais[a][0.05].predict(X) - ajuste[a],
                   0.95: finais[a][0.95].predict(X) + ajuste[a]}
    else:  # ML não superou a média da cultura na validação → usa a média
        m = P.cultura.map(medias[a]).values
        pred[a] = {None: m, 0.05: m - ajuste[a], 0.95: m + ajuste[a]}
# quantis independentes podem se cruzar: garante p05 <= p50 <= p95
for a in pred:
    pred[a][0.05] = np.minimum(pred[a][0.05], pred[a][None])
    pred[a][0.95] = np.maximum(pred[a][0.95], pred[a][None])
P["produtividade_prevista"] = pred["produtividade_t_ha"][None].round(1)
P["produtividade_p05"] = pred["produtividade_t_ha"][0.05].round(1)
P["produtividade_p95"] = pred["produtividade_t_ha"][0.95].round(1)
P["variacao_safra_anterior_pct"] = ((P.produtividade_prevista / P.produtividade_safra_anterior - 1) * 100).round(1)
d_ini = np.maximum(pred["dias_ate_inicio_colheita"][None], 0)
largura = np.abs(pred["dias_ate_inicio_colheita"][0.95] - pred["dias_ate_inicio_colheita"][0.05])
P["colheita_inicio"] = [(HOJE + pd.Timedelta(days=int(round(x)))).date() for x in d_ini]
P["colheita_fim"] = [(pd.Timestamp(i) + pd.Timedelta(days=int(round(max(x, 3))))).date()
                     for i, x in zip(P.colheita_inicio, pred["duracao_colheita"][None])]
# confiança: quanto mais estreito o intervalo de 90% da data de início, maior (30 dias de largura → 50%)
P["confianca_colheita_pct"] = np.clip(100 - largura / 30 * 50, 50, 99).round(0)
P["exportacao_inicio"] = [(pd.Timestamp(i) + pd.Timedelta(days=int(round(max(x, 0))))).date()
                          for i, x in zip(P.colheita_inicio, pred["offset_exportacao"][None])]
P["exportacao_fim"] = [(pd.Timestamp(i) + pd.Timedelta(days=int(round(max(x, 3))))).date()
                       for i, x in zip(P.exportacao_inicio, pred["duracao_exportacao"][None])]


def destino(row):
    hist = saf[(saf.talhao_codigo == row.talhao_codigo) & saf.destino_exportacao.notna()]
    if row.cultura == "Manga":
        return "Filadélfia (EUA)" if pd.Timestamp(row.colheita_inicio).month in (12, 1, 2, 3) else "Roterdã (UE)"
    return hist.destino_exportacao.mode().iloc[0] if len(hist) else None


P["destino_exportacao"] = P.apply(destino, axis=1)


def motivo(row):
    partes = []
    if row.frac_horas_acima_tmax > 0.15:
        partes.append(f"{row.frac_horas_acima_tmax:.0%} das horas acima da faixa de temperatura, o que acelera a maturação")
    if pd.notna(row.vigor_atual):
        partes.append(f"vigor atual {row.vigor_atual:.2f}")
    if row.chuva_prevista_14d >= 20:
        partes.append(f"{row.chuva_prevista_14d:.0f} mm de chuva prevista em 14 dias")
    if pd.notna(row.variacao_safra_anterior_pct):
        partes.append(f"{row.variacao_safra_anterior_pct:+.1f}% vs safra anterior")
    return "; ".join(partes).capitalize()


P["motivo"] = P.apply(motivo, axis=1)

# curva de vigor projetada (de hoje até o fim previsto da colheita)
curvas = []
for _, r in P.iterrows():
    ciclo = CICLO[r.cultura]
    fim = pd.Timestamp(r.colheita_fim)
    for hz in range(7, max(8, (fim - HOJE).days + 8), 7):
        x = pd.DataFrame([dict(cultura_cod=CULTURAS.index(r.cultura), fracao=r.fracao_ciclo, h=hz,
                               vigor_ref=r.vigor_atual, vigor_ref_7=np.nan)])[VF]
        q = {k: float(m.predict(x)[0]) for k, m in vmod.items()}
        curvas.append(dict(talhao_codigo=r.talhao_codigo, data=(HOJE + pd.Timedelta(days=hz)).date(),
                           vigor_previsto=round(q[None], 3), vigor_p05=round(min(q[0.05], q[None]), 3),
                           vigor_p95=round(max(q[0.95], q[None]), 3)))

cols = ["talhao_codigo", "safra_id", "cultura", "variedade", "data_referencia", "produtividade_prevista", "produtividade_p05",
        "produtividade_p95", "produtividade_safra_anterior", "variacao_safra_anterior_pct", "colheita_inicio", "colheita_fim",
        "confianca_colheita_pct", "exportacao_inicio", "exportacao_fim", "destino_exportacao", "motivo", "vigor_atual"]
out = P[cols].copy().merge(spark.table("campo_vivo.silver.talhoes").select("talhao_codigo", "fazenda_id").toPandas(),
                           on="talhao_codigo", how="left")
out["data_referencia"] = out["data_referencia"].dt.date
out["modelo_run_id"] = run_id
out["gerado_em"] = pd.Timestamp.utcnow().isoformat()
salvar(spark.createDataFrame(out.astype(object).where(out.notna(), None)), "gold", "predicoes_talhao",
       comentario="Predição por talhão: produtividade, janela de colheita e de exportação (tela Predições de IA).")
salvar(spark.createDataFrame(pd.DataFrame(curvas)), "gold", "predicoes_vigor_curva",
       comentario="Curva de vigor projetada com intervalo de 90%.")
fat = pd.DataFrame([dict(fator=k, peso_pct=round(v * 100, 1)) for k, v in sorted(fatores.items(), key=lambda x: -x[1])])
salvar(spark.createDataFrame(fat), "gold", "modelo_fatores", comentario="Importância (permutação agrupada) do modelo de produtividade.")
n_sens = spark.table("campo_vivo.silver.sensores").count()
meta = pd.DataFrame([dict(modelo=a, descricao=ALVOS.get(a, "Curva de vigor"), estrategia=estrategia.get(a, "ml"),
                          mae=round(m["mae"], 3), r2=round(m["r2"], 3), cobertura_intervalo_90_bruta=m.get("cobertura_90_bruta"),
                          mae_baseline=round(m.get("mae_baseline", np.nan), 3), cobertura_intervalo_90=m.get("cobertura_90"),
                          n_safras_treino=int(treino_all.safra_id.nunique()), n_sensores=n_sens, mlflow_run_id=run_id,
                          treinado_em=pd.Timestamp.utcnow().isoformat()) for a, m in metricas.items()])
salvar(spark.createDataFrame(meta.astype(object).where(meta.notna(), None)), "gold", "modelo_metadados",
       comentario="Métricas da validação temporal e metadados do treino.")
print(fat)
print(out[["talhao_codigo", "produtividade_prevista", "variacao_safra_anterior_pct", "colheita_inicio", "colheita_fim",
           "confianca_colheita_pct", "exportacao_inicio", "exportacao_fim", "destino_exportacao"]].to_string())
