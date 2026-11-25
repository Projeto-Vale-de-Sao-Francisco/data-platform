"""Gera docs/DICIONARIO_DE_DADOS.md a partir do information_schema do catálogo campo_vivo.

Tipos e colunas vêm do Databricks (sempre atuais); as descrições ficam no dicionário DESCRICOES abaixo.
Uso: DATABRICKS_HOST=… DATABRICKS_TOKEN=… DATABRICKS_WAREHOUSE_ID=… python scripts/gerar_dicionario.py
"""
import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "extrator"))
import extrator as ex  # noqa: E402

DESCRICOES = {
    # linhagem
    "_ingest_ts": "Momento em que a linha entrou na Bronze (UTC)",
    "_source": "Origem do dado: open_meteo, cadastro_frontend, simulador_iot, registro_produtor_simulado, sentinel2_simulado, extrator_externo",
    "_arquivo": "Arquivo de origem no Volume landing",
    # chaves e cadastro
    "id": "Identificador no cadastro de origem",
    "codigo": "Código do talhão (T-01, BV-03…) ou do sensor (SN-101…), conforme a tabela",
    "talhao_codigo": "Código do talhão — chave de junção entre todas as camadas e com o backend",
    "sensor_codigo": "Código do sensor (SN-101…)",
    "fazenda_id": "Fazenda: 1 Santa Helena (Petrolina-PE), 2 Boa Vista (Juazeiro-BA), 3 Três Irmãos (Mossoró-RN)",
    "safra_id": "Safra: talhão + sequência, ex. T-01-07",
    "safra_seq": "Número da safra no talhão, em ordem cronológica",
    "nome": "Nome do registro (fazenda, talhão, produtor…)",
    "cultura": "Cultura: Manga, Uva, Goiaba ou Melão",
    "variedade": "Variedade da cultura (Palmer, Vitória, Paluma…)",
    "variedades": "Variedades cadastradas para a cultura",
    "descricao": "Descrição livre",
    "municipio": "Município",
    "cidade": "Cidade",
    "uf": "Estado (UF)",
    "area_total_ha": "Área total da fazenda (ha)",
    "produtor": "Produtor responsável",
    "status": "Situação do registro (ativo, em_andamento, concluida…)",
    "latitude": "Latitude (graus decimais)",
    "longitude": "Longitude (graus decimais)",
    "poligono": "Contorno do talhão em GeoJSON (Polygon, longitude/latitude)",
    "estagio": "Estágio fenológico atual: poda_inducao, floracao, frutificacao, maturacao, colheita",
    "ph_referencia": "pH de referência do solo do talhão",
    "tipo": "Tipo de sensor: estacao_climatica ou sonda_solo",
    "offline_atual": "Sensor offline no momento do cadastro",
    "bateria": "Bateria do sensor (%)",
    "sinal_dbm": "Intensidade do sinal (dBm)",
    "rssi": "Intensidade do sinal na mensagem (dBm)",
    "firmware": "Versão de firmware do sensor",
    "temperatura_min": "Limite inferior da faixa ideal de temperatura da cultura (°C)",
    "temperatura_max": "Limite superior da faixa ideal de temperatura da cultura (°C)",
    "umidade_min": "Limite inferior da faixa ideal de umidade (%) — em culturas; mínima do dia nas tabelas diárias",
    "umidade_max": "Limite superior da faixa ideal de umidade (%) — em culturas; máxima do dia nas tabelas diárias",
    "ciclo_medio_dias": "Ciclo médio da cultura (dias)",
    "data_plantio_atual": "Data de plantio/poda da safra atual (cadastro)",
    "previsao_colheita_atual": "Previsão de colheita informada no cadastro",
    "previsao_colheita_cadastro": "Previsão de colheita informada no cadastro",
    "doc": "CPF/CNPJ (cadastro de produtores)",
    "tel": "Telefone",
    "email": "E-mail de contato",
    "resp": "Responsável",
    "mercado": "Mercado de destino (cadastro de mercado)",
    "preco": "Preço médio de referência",
    "moeda": "Moeda do preço",
    "demanda": "Demanda estimada",
    "origem": "Origem: archive (reanálise) ou forecast no clima; local de partida na logística",
    "destino": "Destino da carga (logística)",
    "modal": "Modal de transporte",
    "tempo": "Tempo estimado de transporte",
    "custo": "Custo estimado de transporte",
    "transp": "Transportadora",
    "sit": "Situação do transporte",
    "data": "Data (dia local de Brasília nas tabelas diárias)",
    # clima Open-Meteo (bronze, nomes originais da API)
    "time": "Hora UTC como veio da API (texto)",
    "temperature_2m": "Temperatura do ar a 2 m (°C)",
    "relative_humidity_2m": "Umidade relativa do ar a 2 m (%)",
    "precipitation": "Chuva na hora (mm)",
    "soil_moisture_0_to_7cm": "Umidade volumétrica do solo, 0–7 cm (m³/m³)",
    "soil_moisture_7_to_28cm": "Umidade volumétrica do solo, 7–28 cm (m³/m³)",
    "shortwave_radiation": "Radiação solar de onda curta (W/m²)",
    "et0_fao_evapotranspiration": "Evapotranspiração de referência FAO (mm)",
    "wind_speed_10m": "Vento a 10 m (km/h)",
    "temperature_2m_max": "Máxima prevista do dia (°C)",
    "temperature_2m_min": "Mínima prevista do dia (°C)",
    "precipitation_sum": "Chuva prevista no dia (mm)",
    "precipitation_probability_max": "Probabilidade máxima de chuva no dia (%)",
    "emitida_em": "Quando o boletim de previsão foi baixado (UTC)",
    # clima e leituras (silver/gold)
    "ts_utc": "Instante da leitura em UTC (já padronizado)",
    "hora_utc": "Hora cheia UTC (reamostragem)",
    "temperatura": "Temperatura do ar (°C)",
    "umidade": "Umidade relativa do ar (%)",
    "umidade_solo": "Umidade do solo (%)",
    "umidade_solo_m3m3": "Umidade volumétrica do solo do Open-Meteo (m³/m³)",
    "luminosidade": "Luminosidade (lux)",
    "precipitacao": "Chuva registrada pelo sensor (mm)",
    "ph": "pH do solo",
    "temperatura_solo": "Temperatura do solo (°C)",
    "chuva_mm": "Chuva (mm) — na hora ou somada no dia",
    "radiacao_wm2": "Radiação solar (W/m²)",
    "et0_mm": "Evapotranspiração de referência (mm)",
    "vento_kmh": "Vento (km/h)",
    "temp_max": "Temperatura máxima (°C) — do dia medida ou prevista, conforme a tabela",
    "temp_min": "Temperatura mínima (°C) — do dia medida ou prevista, conforme a tabela",
    "chuva_prevista_mm": "Chuva prevista no dia (mm)",
    "prob_chuva_pct": "Probabilidade de chuva (%)",
    "payload": "Mensagem bruta do sensor (JSON), como recebida",
    "received_at": "Quando o gateway recebeu a mensagem (UTC)",
    "gateway": "Gateway ou origem da mensagem (GW-F1, backend-render, thingspeak)",
    "formato_data": "Formato original da data: iso_utc, iso_offset, epoch, local_sem_fuso",
    "problemas": "Lista de problemas tratados na leitura (valor inválido, extremo, campo ausente)",
    "motivo_quarentena": "Por que a mensagem foi rejeitada (sensor_nao_cadastrado, data_ilegivel…)",
    "sensores_ativos": "Quantos sensores do talhão enviaram leitura na hora",
    "problema": "Tipo de problema tratado na carga",
    "count": "Quantidade de ocorrências",
    "total_mensagens": "Total de mensagens da carga",
    "temp_media": "Temperatura média do dia (°C)",
    "umidade_media": "Umidade média do dia (%)",
    "umidade_solo_media": "Umidade do solo média (%) — no dia ou desde o plantio, conforme a tabela",
    "ph_medio": "pH médio do dia",
    "horas_temp_fora_faixa": "Horas do dia com temperatura fora da faixa ideal da cultura",
    "horas_umid_fora_faixa": "Horas do dia com umidade fora da faixa ideal da cultura",
    "horas_condicao_adequada": "Horas do dia com temperatura e umidade dentro da faixa",
    "horas_acima_tmax": "Horas do dia acima do teto de temperatura da cultura",
    "horas_com_leitura": "Horas do dia com leitura válida de temperatura",
    "chuva_ref": "Chuva do dia pelo Open-Meteo (mm), usada quando falta sensor",
    "et0_ref": "Evapotranspiração do dia pelo Open-Meteo (mm)",
    "origem_temp": "De onde veio a temperatura do dia: sensor ou referencia_openmeteo",
    "graus_dia": "Graus-dia do dia: média limitada a 36 °C menos a temperatura base da cultura",
    "completude_pct": "% das 24 horas com leitura válida",
    # safras e vigor
    "data_plantio": "Data de plantio/poda da safra",
    "data_inicio_colheita": "Início real da colheita",
    "data_fim_colheita": "Fim real da colheita",
    "data_inicio_exportacao": "Início real da janela de exportação",
    "data_fim_exportacao": "Fim real da janela de exportação",
    "destino_exportacao": "Destino da exportação (Roterdã, Filadélfia, Londres, CEAGESP, Recife)",
    "produtividade_t_ha": "Produtividade da safra (t/ha)",
    "produtividade_valida": "Produtividade dentro da faixa plausível (1–120 t/ha) ou vazia",
    "dias_ate_colheita": "Dias entre plantio e início da colheita",
    "duracao_colheita": "Duração da colheita (dias)",
    "data_imagem": "Data da imagem de satélite",
    "ndvi": "Índice de vegetação (0 a 1) — vazio quando a imagem tinha nuvem",
    "cobertura_nuvens_pct": "Cobertura de nuvens na imagem (%)",
    "indice_vigor": "Índice de vigor (NDVI) de uma imagem válida",
    "vigor": "Índice de vigor diário (interpolado entre imagens)",
    "observado": "Verdadeiro quando o vigor do dia veio de uma imagem, falso quando interpolado",
    # indicadores
    "indice_condicoes_adequadas_7d": "% das horas dos últimos 7 dias com temperatura e umidade na faixa",
    "horas_temp_fora_faixa_7d": "Horas com temperatura fora da faixa nos últimos 7 dias",
    "horas_umid_fora_faixa_7d": "Horas com umidade fora da faixa nos últimos 7 dias",
    "temp_media_7d": "Temperatura média dos últimos 7 dias (°C)",
    "umidade_solo_7d": "Umidade do solo média dos últimos 7 dias (%)",
    "evolucao_condicoes_pp": "Variação do índice de condições adequadas vs os 7 dias anteriores (pontos percentuais)",
    "tendencia": "melhorando, estavel ou piorando",
    "dias_desde_plantio": "Dias desde o plantio/poda até a data de referência",
    "dias_ate_colheita_cadastro": "Dias até a colheita prevista no cadastro",
    "atualizado_em": "Quando o indicador foi calculado (UTC)",
    # features e alvos
    "data_referencia": "Data em que as features foram observadas (ponto da previsão)",
    "fracao_ciclo": "Dias desde o plantio ÷ ciclo nominal da cultura",
    "umidade_solo_30d": "Umidade do solo média dos 30 dias anteriores à referência (%)",
    "temp_media_30d": "Temperatura média dos 30 dias anteriores à referência (°C)",
    "graus_dia_acum": "Graus-dia acumulados desde o plantio",
    "frac_horas_acima_tmax": "Fração das horas desde o plantio acima do teto de temperatura da cultura",
    "chuva_prevista_14d": "Chuva nos 14 dias seguintes (mm): prevista hoje; observada nas linhas históricas",
    "chuva_acum_30d": "Chuva dos 30 dias anteriores à referência (mm)",
    "produtividade_safra_anterior": "Produtividade da safra anterior do talhão (t/ha)",
    "produtividade_media_talhao": "Média de produtividade das safras anteriores do talhão (t/ha)",
    "dias_colheita_safra_anterior": "Dias até a colheita na safra anterior",
    "vigor_atual": "Último índice de vigor até a referência",
    "vigor_max": "Maior vigor da safra até a referência",
    "vigor_tendencia_30d": "Variação do vigor em 30 dias (inclinação da reta)",
    "dias_ate_inicio_colheita": "ALVO: dias entre a referência e o início da colheita",
    "offset_exportacao": "ALVO: dias entre o início da colheita e o início da exportação",
    "duracao_exportacao": "ALVO: duração da janela de exportação (dias)",
    "conjunto": "treino (safra concluída) ou prever (safra atual)",
    # predições
    "produtividade_prevista": "Produtividade prevista (t/ha), mediana",
    "produtividade_p05": "Limite inferior do intervalo de 90% (t/ha)",
    "produtividade_p95": "Limite superior do intervalo de 90% (t/ha)",
    "variacao_safra_anterior_pct": "Variação da produtividade prevista sobre a safra anterior (%)",
    "colheita_inicio": "Início previsto da colheita",
    "colheita_fim": "Fim previsto da colheita",
    "confianca_colheita_pct": "Confiança na data de colheita (%): 99 = intervalo estreito; 50 = intervalo de 30 dias ou mais",
    "exportacao_inicio": "Início previsto da exportação",
    "exportacao_fim": "Fim previsto da exportação",
    "motivo": "Explicação em texto dos fatores da previsão ou do risco",
    "modelo_run_id": "Execução do MLflow que gerou a previsão",
    "gerado_em": "Quando foi gerado (UTC)",
    "vigor_previsto": "Vigor previsto (mediana)",
    "vigor_p05": "Limite inferior do intervalo de 90% do vigor",
    "vigor_p95": "Limite superior do intervalo de 90% do vigor",
    "fator": "Grupo de variáveis do modelo de produtividade",
    "peso_pct": "Peso do grupo no modelo (% da importância por permutação)",
    "modelo": "Alvo do modelo",
    "estrategia": "ml ou media_por_cultura (quando a regressão não superou a média)",
    "mae": "Erro absoluto médio no teste temporal (unidade do alvo)",
    "r2": "R² no teste temporal",
    "mae_baseline": "Erro absoluto médio usando só a média da cultura",
    "cobertura_intervalo_90": "Fração dos casos de teste dentro do intervalo de 90% calibrado",
    "cobertura_intervalo_90_bruta": "A mesma fração antes da calibração conformal",
    "n_safras_treino": "Safras usadas no treino",
    "n_sensores": "Sensores cadastrados",
    "mlflow_run_id": "Execução do MLflow",
    "treinado_em": "Quando o modelo foi treinado (UTC)",
    # risco
    "temp_max_prevista": "Maior máxima prevista nos próximos 7 dias (°C)",
    "tmax_p90_historico": "Percentil 90 das máximas diárias da fazenda no mesmo mês, 2019→ (°C)",
    "chuva_7d_mm": "Chuva prevista nos próximos 7 dias (mm)",
    "dias_calor": "Dias de calor acima do normal nos próximos 7 dias",
    "dias_frio": "Dias com média abaixo do piso da cultura nos próximos 7 dias",
    "dias_chuva_forte": "Dias com chuva prevista ≥ 20 mm",
    "colheita_proxima": "Colheita prevista para os próximos 21 dias",
    "pontos": "Pontuação de risco: 2 por dia de calor ou frio + 3 por dia de chuva forte perto da colheita",
    "nivel": "Baixo (0–2), Moderado (3–7) ou Alto (≥ 8)",
    "talhoes_em_risco": "Talhões da fazenda com risco Moderado ou Alto",
    "resumo": "Resumo do risco da fazenda",
}

ANALYTICS = {
    "predicoes_talhao": "gold.predicoes_talhao", "predicoes_vigor_curva": "gold.predicoes_vigor_curva",
    "vigor_medido": "gold.vigor_diario (últimos 120 dias)", "modelo_fatores": "gold.modelo_fatores",
    "modelo_metadados": "gold.modelo_metadados", "risco_climatico_talhao": "gold.risco_climatico_talhao",
    "risco_climatico_fazenda": "gold.risco_climatico_fazenda", "indicadores_talhao": "gold.indicadores_talhao",
}
CAMADAS = {
    "bronze": "Dados brutos, só acrescentados, com linhagem (`_ingest_ts`, `_source`).",
    "silver": "Dados limpos: tipos corretos, UTC, sem duplicados, inválidos e extremos tratados.",
    "gold": "Agregados por talhão, indicadores, features e previsões consumidas pela aplicação.",
}


def main():
    _, cols = ex.consultar("SELECT table_schema, table_name, column_name, full_data_type FROM campo_vivo.information_schema.columns "
                           "WHERE table_schema IN ('bronze','silver','gold') ORDER BY table_schema, table_name, ordinal_position")
    _, tabs = ex.consultar("SELECT table_schema, table_name, comment FROM campo_vivo.information_schema.tables "
                           "WHERE table_schema IN ('bronze','silver','gold')")
    comentario = {(s, t): c for s, t, c in tabs}
    faltando = sorted({c for _, _, c, _ in cols if c not in DESCRICOES})
    linhas = ["# Dicionário de dados · catálogo `campo_vivo`", "",
              "Gerado por `scripts/gerar_dicionario.py` a partir do `information_schema` do Databricks: tipos e colunas "
              "refletem o catálogo no momento da geração. Datas e horas em UTC, salvo indicação.", "",
              "## Índice", ""]
    ordem = ["bronze", "silver", "gold"]
    for camada in ordem:
        nomes = sorted({t for s, t, _, _ in cols if s == camada})
        linhas.append(f"- **{camada}** — " + ", ".join(f"[{t}](#{camada}{t})" for t in nomes))
    linhas += ["- **analytics** (Postgres) — [tabelas publicadas](#postgres-analytics)", ""]
    for camada in ordem:
        linhas += [f"## {camada.capitalize()}", "", CAMADAS[camada], ""]
        for t in sorted({t for s, t, _, _ in cols if s == camada}):
            linhas += [f"### {camada}.{t}", ""]
            if comentario.get((camada, t)):
                linhas += [comentario[(camada, t)], ""]
            linhas += ["| Coluna | Tipo | Descrição |", "| --- | --- | --- |"]
            linhas += [f"| `{c}` | {tipo.lower()} | {DESCRICOES.get(c, '')} |" for s, tt, c, tipo in cols if s == camada and tt == t]
            linhas.append("")
    linhas += ["## Postgres `analytics`", "",
               "Cópias das tabelas Gold gravadas no Postgres (Aiven) pelo extrator, com as mesmas colunas e tipos "
               "equivalentes. Recriadas por troca atômica a cada publicação.", "",
               "| Tabela no Postgres | Origem no Databricks |", "| --- | --- |"]
    linhas += [f"| `analytics.{k}` | `campo_vivo.{v}` |" for k, v in ANALYTICS.items()]
    (RAIZ / "docs" / "DICIONARIO_DE_DADOS.md").write_text("\n".join(linhas) + "\n", encoding="utf-8")
    print(f"{len(cols)} colunas em {len({(s, t) for s, t, _, _ in cols})} tabelas; sem descrição: {faltando or 'nenhuma'}")


if __name__ == "__main__":
    main()
