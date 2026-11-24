import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "extrator"))
import extrator as ex  # noqa: E402


def test_medicao_backend_vira_payload_padrao():
    m = {"sensor_id": 1, "data_hora": "2026-10-12T22:23:59.000Z", "temperatura": 27.5, "umidade": 62.2,
         "umidade_solo": None, "sensor": {"dispositivo": {"codigo": "ESP32-TALHAO-01"}}}
    p = ex.medicao_backend_para_payload(m)
    assert p == {"device": "ESP32-TALHAO-01", "ts": "2026-10-12T22:23:59.000Z", "temperatura": 27.5, "umidade": 62.2}


def test_medicao_sem_dispositivo_usa_id_do_sensor():
    assert ex.medicao_backend_para_payload({"sensor_id": 7, "data_hora": "x"})["device"] == "SENSOR-7"


def test_feed_thingspeak_converte_texto_e_ignora_vazios():
    p = ex.feed_thingspeak_para_payload({"created_at": "2026-11-24T10:00:00Z", "field1": "31.4", "field2": ""}, "ESP32-T1")
    assert p == {"device": "ESP32-T1", "ts": "2026-11-24T10:00:00Z", "temperatura": 31.4}
    json.dumps(p)


def test_tipos_postgres_conhecidos():
    assert ex.TIPOS_PG["DOUBLE"] == "double precision" and ex.TIPOS_PG["DATE"] == "date"
