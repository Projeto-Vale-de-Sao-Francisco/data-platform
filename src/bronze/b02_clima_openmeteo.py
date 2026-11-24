"""Bronze 02 · Clima Open-Meteo (histórico ERA5 horário + previsão de 16 dias) por fazenda.

Resposta bruta de cada chamada é guardada em /Volumes/.../landing/open_meteo/ e também
"achatada" em tabelas Delta append-only. Tempo em UTC (timezone=GMT).
"""
import sys, json, datetime as dt, urllib.request, urllib.parse, os
sys.path.insert(0, "/Volumes/campo_vivo/bronze/code")
from common import spark, F, LANDING, INICIO_HISTORICO, com_metadados, salvar

HORARIAS = ["temperature_2m", "relative_humidity_2m", "precipitation", "soil_moisture_0_to_7cm",
            "soil_moisture_7_to_28cm", "shortwave_radiation", "et0_fao_evapotranspiration", "wind_speed_10m"]
DIARIAS = ["temperature_2m_max", "temperature_2m_min", "precipitation_sum",
           "precipitation_probability_max", "et0_fao_evapotranspiration"]

fazendas = [r.asDict() for r in spark.table("campo_vivo.bronze.cad_fazendas")
            .select("id", "nome", "latitude", "longitude").collect()]
hoje = dt.date.today()
carimbo = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%S")
os.makedirs(f"{LANDING}/open_meteo", exist_ok=True)


def baixar(url: str, params: dict, arquivo: str) -> dict:
    full = url + "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(urllib.request.Request(full, headers={"User-Agent": "campo-vivo"}), timeout=120) as r:
        corpo = r.read().decode()
    with open(f"{LANDING}/open_meteo/{arquivo}", "w") as f:
        f.write(corpo)
    return json.loads(corpo)


linhas_h, linhas_d = [], []
for fz in fazendas:
    base = {"latitude": fz["latitude"], "longitude": fz["longitude"], "timezone": "GMT"}
    # 1) histórico (reanálise): até 6 dias atrás
    arq = baixar("https://archive-api.open-meteo.com/v1/archive",
                 {**base, "start_date": INICIO_HISTORICO, "end_date": str(hoje - dt.timedelta(days=6)),
                  "hourly": ",".join(HORARIAS)},
                 f"archive_f{fz['id']}_{carimbo}.json")
    # 2) previsão com 10 dias passados para cobrir a defasagem do arquivo
    prev = baixar("https://api.open-meteo.com/v1/forecast",
                  {**base, "past_days": 10, "forecast_days": 16,
                   "hourly": ",".join(HORARIAS), "daily": ",".join(DIARIAS)},
                  f"forecast_f{fz['id']}_{carimbo}.json")
    for origem, js in (("archive", arq), ("forecast", prev)):
        h = js["hourly"]
        for i, t in enumerate(h["time"]):
            linhas_h.append((fz["id"], origem, t, *[h.get(v, [None] * len(h["time"]))[i] for v in HORARIAS]))
    d = prev["daily"]
    for i, t in enumerate(d["time"]):
        linhas_d.append((fz["id"], t, carimbo, *[d.get(v, [None] * len(d["time"]))[i] for v in DIARIAS]))
    print(f"fazenda {fz['id']}: {len(arq['hourly']['time'])} horas de histórico, {len(d['time'])} dias de previsão")

esq_h = "fazenda_id INT, origem STRING, time STRING, " + ", ".join(f"{v} DOUBLE" for v in HORARIAS)
esq_d = "fazenda_id INT, data STRING, emitida_em STRING, " + ", ".join(f"{v} DOUBLE" for v in DIARIAS)

salvar(com_metadados(spark.createDataFrame(linhas_h, esq_h), "open_meteo"), "bronze", "clima_openmeteo_horario",
       modo="append", comentario="Clima horário Open-Meteo (archive ERA5 + forecast), bruto, append-only, UTC.")
salvar(com_metadados(spark.createDataFrame(linhas_d, esq_d), "open_meteo"), "bronze", "clima_previsao_diaria",
       modo="append", comentario="Previsão diária Open-Meteo (16 dias), um snapshot por execução.")
