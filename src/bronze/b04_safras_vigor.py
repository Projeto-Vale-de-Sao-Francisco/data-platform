"""Bronze 04 · Histórico de safras (registro do produtor) e índice de vigor por satélite — simulados.

Modelo agronômico usado para gerar o histórico (documentado para a banca):
  * Colheita começa quando a soma de graus-dia (GDD) desde a poda/plantio atinge a meta da cultura:
    safras mais quentes colhem antes.
  * Vigor de pico cai com horas acima da temperatura máxima da cultura, déficit hídrico no solo e
    chuva na maturação; a curva de vigor sobe (logística), atinge o pico e cai na colheita.
  * Produtividade (t/ha) = base da cultura × f(vigor de pico) × penalidade de chuva na maturação
    × fator do talhão × ruído.
  * Exportação: destino por cultura/variedade/época; início = colheita + packing + espera do navio
    semanal; fim = fim da colheita + mesmo lead + armazenagem refrigerada.
O vigor é "observado" a cada 5 dias (revisita Sentinel-2), com falhas por nuvem.
"""
import sys, zlib, datetime as dt
import numpy as np
import pandas as pd
sys.path.insert(0, "/Volumes/campo_vivo/bronze/code")
from common import (spark, F, com_metadados, salvar, clima_horario_base, offset_temp_talhao,
                    alvo_solo_talhao, umidade_solo_expr)

HOJE = pd.Timestamp(dt.date.today())
PARAM = {  # período entre safras, ciclo nominal, T base GDD, duração colheita, produtividade base, vigor de repouso
    "Manga":  dict(periodo=365, ciclo=145, tbase=13, dur=12, base=33, v0=0.50),
    "Uva":    dict(periodo=182, ciclo=110, tbase=10, dur=10, base=34, v0=0.28),
    "Goiaba": dict(periodo=210, ciclo=180, tbase=12, dur=14, base=50, v0=0.45),
    "Melão":  dict(periodo=365, ciclo=70,  tbase=12, dur=9,  base=32, v0=0.15),
}
ROTA = {  # destino, dias de packing + estrada até o porto/mercado, dia da semana do navio (0=seg) ou None
    "Roterdã (UE)": (2, 3), "Filadélfia (EUA)": (2, 1), "Londres (UK)": (1, 0),
    "CEAGESP (SP)": (2, None), "Recife (PE)": (1, None),
}


def destino(cultura, variedade, inicio):
    if cultura == "Manga":
        return "Filadélfia (EUA)" if inicio.month in (12, 1, 2, 3) else "Roterdã (UE)"
    if cultura == "Uva":
        return "CEAGESP (SP)" if variedade == "Vitória" else "Roterdã (UE)"
    if cultura == "Goiaba":
        return "Recife (PE)"
    return "Roterdã (UE)" if variedade == "Pele de Sapo" else "Londres (UK)"


def lead(dest, dia):
    pack, navio = ROTA[dest]
    d = dia + pd.Timedelta(days=pack)
    if navio is not None:
        d = d + pd.Timedelta(days=(navio - d.weekday()) % 7)
    return d


# ---------- clima diário "verdadeiro" por talhão (mesmo microclima do simulador IoT) ----------
clima = clima_horario_base()
sm_media = clima.groupBy("fazenda_id").agg(F.avg("soil_moisture_0_to_7cm").alias("sm_media"))
cult = spark.table("campo_vivo.bronze.cad_culturas").select(F.col("nome").alias("cultura"), "temperatura_max")
tal = spark.table("campo_vivo.bronze.cad_talhoes").select("codigo", "fazenda_id", "cultura", "variedade",
                                                            "data_plantio_atual", "previsao_colheita_atual")
diario = (tal.join(cult, "cultura").join(clima, "fazenda_id").join(sm_media, "fazenda_id")
          .withColumn("t", F.col("temperature_2m") + offset_temp_talhao(F.col("codigo")))
          .withColumn("solo", umidade_solo_expr(alvo_solo_talhao(F.col("codigo")), F.col("soil_moisture_0_to_7cm"),
                                                F.col("sm_media"), F.col("temperature_2m")))
          .withColumn("alvo", alvo_solo_talhao(F.col("codigo")))
          .groupBy("codigo", F.to_date("ts").alias("data"))
          .agg(F.avg("t").alias("tmed"), F.sum(F.when(F.col("t") > F.col("temperatura_max"), 1).otherwise(0)).alias("h_quente"),
               F.avg("solo").alias("solo"), F.first("alvo").alias("alvo"), F.sum("precipitation").alias("chuva"))
          ).toPandas()
diario["data"] = pd.to_datetime(diario["data"])
talhoes = tal.toPandas()
rng_global = np.random.default_rng(2026)

safras, vigor = [], []
for _, t in talhoes.iterrows():
    p = PARAM[t.cultura]
    d = diario[diario.codigo == t.codigo].set_index("data").sort_index()
    gdd = (d.tmed.clip(upper=36) - p["tbase"]).clip(lower=0)
    meta_gdd = p["ciclo"] * gdd.mean()
    rng = np.random.default_rng(zlib.crc32(t.codigo.encode()))
    fator_talhao = rng.uniform(0.9, 1.1)
    plantio_atual = pd.Timestamp(t.data_plantio_atual)
    k, linhas = 0, []
    while True:
        plantio = plantio_atual - pd.Timedelta(days=k * p["periodo"] + (int(rng.integers(-7, 8)) if k else 0))
        if plantio < d.index.min() + pd.Timedelta(days=5):
            break
        linhas.append(plantio)
        k += 1
    for i, plantio in enumerate(sorted(linhas)):
        atual = plantio == plantio_atual
        acum = gdd[plantio:].cumsum()
        atingiu = acum[acum >= meta_gdd]
        inicio = atingiu.index[0] if len(atingiu) else None
        if inicio is None or inicio > HOJE or atual:
            fim_obs = HOJE
        else:
            fim_obs = inicio
        janela = d[plantio:fim_obs]
        horas = max(len(janela) * 24, 1)
        frac_quente = janela.h_quente.sum() / horas
        deficit = (janela.alvo - janela.solo).clip(lower=0).mean()
        chuva_mat = d[fim_obs - pd.Timedelta(days=25):fim_obs].chuva.sum()
        vpico = float(np.clip(0.95 - 0.8 * frac_quente - 0.015 * deficit - 0.0007 * chuva_mat
                              + 0.03 * rng.standard_normal(), 0.40, 0.93))
        concluida = (inicio is not None) and (inicio <= HOJE) and not atual
        reg = dict(talhao_codigo=t.codigo, safra_seq=i + 1, cultura=t.cultura, variedade=t.variedade,
                   data_plantio=plantio.date(), status="concluida" if concluida else "em_andamento",
                   data_inicio_colheita=None, data_fim_colheita=None, produtividade_t_ha=None,
                   destino_exportacao=None, data_inicio_exportacao=None, data_fim_exportacao=None)
        if concluida:
            prod = (p["base"] * (0.45 + 0.7 * vpico) * (1 - 0.003 * chuva_mat) * fator_talhao
                    * (1 + 0.04 * rng.standard_normal()))
            dur = int(max(5, round(p["dur"] + 2 * rng.standard_normal() + 10 * (vpico - 0.75))))
            fim = inicio + pd.Timedelta(days=dur)
            dest = destino(t.cultura, t.variedade, inicio)
            exp_ini = lead(dest, inicio)
            exp_fim = lead(dest, fim) + pd.Timedelta(days=int(rng.integers(2, 6)))
            reg.update(data_inicio_colheita=inicio.date(), data_fim_colheita=fim.date(),
                       produtividade_t_ha=round(float(prod), 2), destino_exportacao=dest,
                       data_inicio_exportacao=exp_ini.date(), data_fim_exportacao=exp_fim.date())
        safras.append(reg)
        # curva de vigor diária (verdade latente) da safra
        fim_curva = (pd.Timestamp(reg["data_fim_colheita"]) if concluida else HOJE)
        T = max((pd.Timestamp(inicio) if inicio is not None and concluida else plantio + pd.Timedelta(days=p["ciclo"])) - plantio,
                pd.Timedelta(days=30)).days
        for dia in pd.date_range(plantio, fim_curva, freq="D"):
            x = (dia - plantio).days / T
            v = p["v0"] + (vpico - p["v0"]) / (1 + np.exp(-10 * (x - 0.35)))
            if x > 0.8:
                v *= 1 - 0.35 * min((x - 0.8) / 0.2, 1.3)
            vigor.append((t.codigo, dia.date(), float(v)))

saf = pd.DataFrame(safras)
vig = pd.DataFrame(vigor, columns=["talhao_codigo", "data", "vigor"]).drop_duplicates(["talhao_codigo", "data"], keep="last")
# observação por satélite: a cada 5 dias, nuvem frequente no período chuvoso (jan–abr)
vig["data"] = pd.to_datetime(vig["data"])
obs = vig[(vig["data"] - pd.Timestamp("2019-01-01")).dt.days % 5 == 0].copy()
p_nuvem = np.where(obs["data"].dt.month.isin([1, 2, 3, 4]), 0.35, 0.08)
nuvem = rng_global.random(len(obs)) < p_nuvem
obs["cobertura_nuvens_pct"] = np.where(nuvem, rng_global.uniform(60, 100, len(obs)), rng_global.uniform(0, 20, len(obs))).round(0)
obs["ndvi"] = np.where(nuvem, np.nan, (obs["vigor"] + rng_global.normal(0, 0.02, len(obs))).clip(0, 1).round(3))
obs = obs.drop(columns="vigor").rename(columns={"data": "data_imagem"})
obs["data_imagem"] = obs["data_imagem"].dt.date

salvar(com_metadados(spark.createDataFrame(saf.astype(object).where(saf.notna(), None)), "registro_produtor_simulado"),
       "bronze", "safras_historico", comentario="Histórico de safras por talhão (simulado, modelo GDD + estresse).")
salvar(com_metadados(spark.createDataFrame(obs.astype(object).where(obs.notna(), None)), "sentinel2_simulado"),
       "bronze", "vigor_satelite", comentario="Índice de vigor (NDVI) por talhão a cada 5 dias, com falhas por nuvem (simulado).")
print(saf.groupby(["cultura", "status"]).size())
print(saf[saf.status == "concluida"].groupby("cultura").produtividade_t_ha.describe())
