"""Extrator externo do Campo Vivo (roda no GitHub Actions).

O Databricks Free Edition não alcança o Render nem o ThingSpeak, então a integração é feita daqui:

  entrada  → busca leituras do backend (Render) e do ThingSpeak, converte para o payload IoT padrão
             e envia um arquivo JSON Lines para /Volumes/campo_vivo/bronze/landing/iot_externo/.
  pipeline → dispara o job `campo_vivo_pipeline_medalhao` e espera terminar.
  saida    → lê as tabelas Gold de predição (SQL Statement API) e grava no Postgres do Render,
             no schema `analytics` (separado das tabelas do Prisma).

  mercado  → baixa ComexStat (preço médio de exportação por país e mês) e PTAX diária do Banco Central
             e envia as respostas brutas para /Volumes/campo_vivo/bronze/landing/mercado/.

Uso:  python extrator.py entrada | mercado | pipeline | saida
Variáveis: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID, DATABRICKS_JOB_ID, BACKEND_URL,
           PIPELINE_API_KEY (header x-pipeline-key), THINGSPEAK_CHANNEL_ID, THINGSPEAK_READ_API_KEY, THINGSPEAK_DEVICE, DATABASE_URL
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

LANDING = "/Volumes/campo_vivo/bronze/landing/iot_externo"
TABELAS_SAIDA = {  # tabela Gold → (tabela no Postgres, colunas-chave)
    "predicoes_talhao": "predicoes_talhao",
    "predicoes_vigor_curva": "predicoes_vigor_curva",
    "vigor_recente": "vigor_medido",
    "modelo_fatores": "modelo_fatores",
    "modelo_metadados": "modelo_metadados",
    "risco_climatico_talhao": "risco_climatico_talhao",
    "risco_climatico_fazenda": "risco_climatico_fazenda",
    "indicadores_talhao": "indicadores_talhao",
    "mercado_serie": "mercado_serie",
    "mercado_referencias": "mercado_referencias",
    "mercado_previsao": "mercado_previsao",
    "mercado_modelo_metadados": "mercado_modelo_metadados",
}


def env(nome: str, obrigatorio: bool = True) -> str | None:
    v = os.environ.get(nome)
    if obrigatorio and not v:
        sys.exit(f"variável de ambiente ausente: {nome}")
    return v


def http(metodo: str, url: str, corpo=None, headers=None, bruto: bytes | None = None, timeout=120):
    data = bruto if bruto is not None else (json.dumps(corpo).encode() if corpo is not None else None)
    req = urllib.request.Request(url, data=data, method=metodo, headers={"User-Agent": "campo-vivo-extrator", **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        txt = r.read().decode()
        return json.loads(txt) if txt.strip().startswith(("{", "[")) else txt


def dbx(metodo: str, caminho: str, corpo=None, bruto=None):
    h = {"Authorization": f"Bearer {env('DATABRICKS_TOKEN').strip()}", "Content-Type": "application/json"}
    try:
        return http(metodo, env("DATABRICKS_HOST").strip().rstrip("/") + caminho, corpo, h, bruto)
    except urllib.error.HTTPError as e:  # mostra a mensagem do Databricks (ex.: escopo do token insuficiente)
        sys.exit(f"Databricks {metodo} {caminho.split('?')[0]} → HTTP {e.code}: {e.read().decode(errors='replace')[:400]}")


# ---------------------------------------------------------------- transformação (testada em tests/)
def medicao_backend_para_payload(m: dict) -> dict:
    """Medição do NestJS (GET /readings) → payload IoT padrão da Bronze."""
    disp = (m.get("sensor") or {}).get("dispositivo") or {}
    return {k: v for k, v in {
        "device": disp.get("codigo") or f"SENSOR-{m.get('sensor_id')}",
        "ts": m.get("data_hora"),
        "temperatura": m.get("temperatura"), "umidade": m.get("umidade"), "umidade_solo": m.get("umidade_solo"),
        "luminosidade": m.get("luminosidade"), "precipitacao": m.get("precipitacao"),
    }.items() if v is not None}


CAMPOS_THINGSPEAK = {"field1": "temperatura", "field2": "umidade", "field3": "umidade_solo",
                     "field4": "luminosidade", "field5": "precipitacao"}


def feed_thingspeak_para_payload(f: dict, device: str) -> dict:
    """Feed do ThingSpeak → payload IoT padrão (mapeamento em CAMPOS_THINGSPEAK; campos vazios são omitidos)."""
    def num(x):
        try:
            return float(x) if x not in (None, "") else None
        except ValueError:
            return None
    p = {"device": device, "ts": f.get("created_at"), **{nome: num(f.get(campo)) for campo, nome in CAMPOS_THINGSPEAK.items()}}
    return {k: v for k, v in p.items() if v is not None}


# ---------------------------------------------------------------- comandos
def _fonte(nome, fn):
    """Executa uma fonte de leituras; uma fonte com erro não impede as outras."""
    try:
        r = fn()
        print(f"[{nome}] {len(r)} mensagens")
        return r, None
    except urllib.error.HTTPError as e:
        detalhe = e.read().decode(errors="replace")[:300]
        print(f"::warning::[{nome}] HTTP {e.code}: {detalhe or e.reason}")
        return [], f"{nome}: HTTP {e.code}"
    except Exception as e:  # noqa: BLE001
        print(f"::warning::[{nome}] {type(e).__name__}: {e}")
        return [], f"{nome}: {type(e).__name__}"


def _backend(agora):
    backend = env("BACKEND_URL")
    inicio = (agora - dt.timedelta(days=2)).isoformat()
    q = urllib.parse.urlencode({"dataInicio": inicio, "limit": 1000})
    # chave de serviço só de leitura, aceita pelo backend apenas em GET /readings (independente da DEVICE_API_KEY)
    h = {"x-pipeline-key": os.environ["PIPELINE_API_KEY"].strip()} if os.environ.get("PIPELINE_API_KEY") else {}
    return [{"payload": json.dumps(medicao_backend_para_payload(m)), "gateway": "backend-render"}
            for m in http("GET", f"{backend.rstrip('/')}/readings?{q}", headers=h, timeout=180)]


def _thingspeak():
    canal = env("THINGSPEAK_CHANNEL_ID").strip()
    chave = (os.environ.get("THINGSPEAK_READ_API_KEY") or "").strip()
    q = urllib.parse.urlencode({"results": 8000, **({"api_key": chave} if chave else {})})
    feeds = http("GET", f"https://api.thingspeak.com/channels/{canal}/feeds.json?{q}").get("feeds", [])
    dev = (os.environ.get("THINGSPEAK_DEVICE") or "ESP32-TALHAO-01").strip()
    return [{"payload": json.dumps(feed_thingspeak_para_payload(f, dev)), "gateway": "thingspeak"} for f in feeds]


def entrada():
    agora = dt.datetime.now(dt.timezone.utc)
    linhas, falhas = [], []
    fontes = []
    if os.environ.get("BACKEND_URL"):
        fontes.append(("backend", lambda: _backend(agora)))
    if os.environ.get("THINGSPEAK_CHANNEL_ID"):
        fontes.append(("thingspeak", _thingspeak))
    for nome, fn in fontes:
        r, erro = _fonte(nome, fn)
        linhas += r
        if erro:
            falhas.append(erro)
    recebido = agora.isoformat()
    corpo = "\n".join(json.dumps({**l, "received_at": recebido}) for l in linhas).encode()
    nome = f"{LANDING}/{agora:%Y%m%dT%H%M%S}.jsonl"
    if linhas:
        dbx("PUT", f"/api/2.0/fs/files{nome}?overwrite=true", bruto=corpo)
    print(f"{len(linhas)} mensagens enviadas para {nome if linhas else '(nada a enviar)'}")
    if fontes and len(falhas) == len(fontes):
        sys.exit("todas as fontes falharam: " + "; ".join(falhas))


LANDING_MERCADO = "/Volumes/campo_vivo/bronze/landing/mercado"
NCM_CULTURAS = {"Manga": ["08045020"], "Uva": ["08061000"], "Melão": ["08071900"], "Goiaba": ["08045010"]}
ESTADOS_COMEX = [26, 29, 24]  # PE, BA, RN (código IBGE da UF, como a ComexStat usa)
INICIO_MERCADO = dt.date(2019, 1, 1)


def _comexstat(hoje: dt.date) -> bytes:
    fim = (hoje.replace(day=1) - dt.timedelta(days=1)).strftime("%Y-%m")
    respostas = {}
    for cultura, ncm in NCM_CULTURAS.items():
        corpo = {"flow": "export", "monthDetail": True,
                 "period": {"from": INICIO_MERCADO.strftime("%Y-%m"), "to": fim},
                 "filters": [{"filter": "ncm", "values": ncm}, {"filter": "state", "values": ESTADOS_COMEX}],
                 "details": ["country"], "metrics": ["metricFOB", "metricKG"]}
        r = http("POST", "https://api-comexstat.mdic.gov.br/general?language=pt", corpo,
                 {"Content-Type": "application/json", "Accept": "application/json"}, timeout=180)
        if not (isinstance(r, dict) and r.get("success") is True):
            raise RuntimeError(f"ComexStat sem sucesso para {cultura}")
        respostas[cultura] = {"ncm": ncm, "estados": ESTADOS_COMEX, "periodo": corpo["period"], "resposta": r}
        print(f"[comexstat] {cultura}: {len(r.get('data', {}).get('list', []))} linhas")
        time.sleep(2)  # a API pública limita requisições seguidas
    return json.dumps(respostas, ensure_ascii=False).encode()


def _ptax(hoje: dt.date) -> bytes:
    fmt = lambda d: d.strftime("%m-%d-%Y")  # noqa: E731 — OData PTAX usa MM-DD-YYYY
    q = urllib.parse.urlencode({"@dataInicial": f"'{fmt(INICIO_MERCADO)}'", "@dataFinalCotacao": f"'{fmt(hoje)}'",
                                "$format": "json", "$top": "10000"})
    r = http("GET", "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
                    f"CotacaoDolarPeriodo(dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)?{q}",
             headers={"Accept": "application/json"}, timeout=180)
    print(f"[ptax] {len(r.get('value', []))} cotações")
    return json.dumps(r).encode()


def mercado():
    hoje = dt.date.today()
    carimbo = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S")
    falhas = 0
    for nome, fn in (("comexstat", _comexstat), ("ptax", _ptax)):
        try:
            corpo = fn(hoje)
        except Exception as e:  # noqa: BLE001
            detalhe = e.read().decode(errors="replace")[:300] if isinstance(e, urllib.error.HTTPError) else str(e)
            print(f"::warning::[{nome}] {type(e).__name__}: {detalhe}")
            falhas += 1
            continue
        dbx("PUT", f"/api/2.0/fs/files{LANDING_MERCADO}/{nome}_{carimbo}.json?overwrite=true", bruto=corpo)
        print(f"[{nome}] enviado para {LANDING_MERCADO}/{nome}_{carimbo}.json")
    if falhas == 2:
        sys.exit("ComexStat e PTAX falharam")


def pipeline():
    run = dbx("POST", "/api/2.2/jobs/run-now", {"job_id": int(env("DATABRICKS_JOB_ID"))})
    rid = run["run_id"]
    print("run", rid)
    while True:
        s = dbx("GET", f"/api/2.2/jobs/runs/get?run_id={rid}")["state"]
        if s.get("life_cycle_state") in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
            print(s)
            if s.get("result_state") != "SUCCESS":
                sys.exit(1)
            return
        time.sleep(30)


TIPOS_PG = {"INT": "integer", "LONG": "bigint", "SHORT": "smallint", "DOUBLE": "double precision", "FLOAT": "real",
            "DECIMAL": "numeric", "DATE": "date", "TIMESTAMP": "timestamptz", "BOOLEAN": "boolean"}


def consultar(sql: str) -> tuple[list[tuple[str, str]], list[list]]:
    r = dbx("POST", "/api/2.0/sql/statements", {"warehouse_id": env("DATABRICKS_WAREHOUSE_ID"), "statement": sql,
                                                "wait_timeout": "50s", "on_wait_timeout": "CONTINUE", "format": "JSON_ARRAY"})
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        r = dbx("GET", f"/api/2.0/sql/statements/{r['statement_id']}")
    if r["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(r["status"])
    cols = [(c["name"], TIPOS_PG.get(c["type_name"], "text")) for c in r["manifest"]["schema"]["columns"]]
    linhas = list(r.get("result", {}).get("data_array", []) or [])
    link = r.get("result", {}).get("next_chunk_internal_link")
    while link:  # resultados grandes vêm em blocos
        bloco = dbx("GET", link)
        linhas += bloco.get("data_array", []) or []
        link = bloco.get("next_chunk_internal_link")
    return cols, linhas


def saida():
    import psycopg  # instalado no workflow

    with psycopg.connect(env("DATABASE_URL")) as conn, conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS analytics")
        for origem, destino in TABELAS_SAIDA.items():
            if origem == "vigor_recente":
                sql = ("SELECT talhao_codigo, data, vigor, observado FROM campo_vivo.gold.vigor_diario "
                       "WHERE data >= date_sub(current_date(), 120)")
            else:
                sql = f"SELECT * FROM campo_vivo.gold.{origem}"
            cols, linhas = consultar(sql)
            cur.execute(f"DROP TABLE IF EXISTS analytics.{destino}_novo")
            cur.execute(f"CREATE TABLE analytics.{destino}_novo ({', '.join(f'{c} {t}' for c, t in cols)})")
            if linhas:
                cur.executemany(f"INSERT INTO analytics.{destino}_novo VALUES ({', '.join(['%s'] * len(cols))})", linhas)
            # troca atômica: a API nunca lê tabela pela metade
            cur.execute(f"DROP TABLE IF EXISTS analytics.{destino}")
            cur.execute(f"ALTER TABLE analytics.{destino}_novo RENAME TO {destino}")
            print(f"analytics.{destino}: {len(linhas)} linhas")
        conn.commit()


if __name__ == "__main__":
    {"entrada": entrada, "mercado": mercado, "pipeline": pipeline, "saida": saida}[sys.argv[1]]()
