# Dicionário de dados · catálogo `campo_vivo`

Gerado por `scripts/gerar_dicionario.py` a partir do `information_schema` do Databricks: tipos e colunas refletem o catálogo no momento da geração. Datas e horas em UTC, salvo indicação.

## Índice

- **bronze** — [cad_culturas](#bronzecad_culturas), [cad_fazendas](#bronzecad_fazendas), [cad_logistica](#bronzecad_logistica), [cad_mercado](#bronzecad_mercado), [cad_produtores](#bronzecad_produtores), [cad_sensores](#bronzecad_sensores), [cad_talhoes](#bronzecad_talhoes), [clima_openmeteo_horario](#bronzeclima_openmeteo_horario), [clima_previsao_diaria](#bronzeclima_previsao_diaria), [leituras_iot](#bronzeleituras_iot), [leituras_iot_externo](#bronzeleituras_iot_externo), [mercado_precos_diarios](#bronzemercado_precos_diarios), [safras_historico](#bronzesafras_historico), [vigor_satelite](#bronzevigor_satelite)
- **silver** — [clima_horario](#silverclima_horario), [culturas](#silverculturas), [fazendas](#silverfazendas), [leituras](#silverleituras), [leituras_horarias](#silverleituras_horarias), [leituras_quarentena](#silverleituras_quarentena), [mercado_fretes](#silvermercado_fretes), [mercado_precos](#silvermercado_precos), [previsao_diaria](#silverprevisao_diaria), [qualidade_leituras](#silverqualidade_leituras), [safras](#silversafras), [sensores](#silversensores), [talhoes](#silvertalhoes), [vigor](#silvervigor)
- **gold** — [clima_diario_talhao](#goldclima_diario_talhao), [features_safra](#goldfeatures_safra), [indicadores_talhao](#goldindicadores_talhao), [mercado_modelo_metadados](#goldmercado_modelo_metadados), [mercado_previsao](#goldmercado_previsao), [mercado_referencias](#goldmercado_referencias), [mercado_serie](#goldmercado_serie), [modelo_fatores](#goldmodelo_fatores), [modelo_metadados](#goldmodelo_metadados), [predicoes_talhao](#goldpredicoes_talhao), [predicoes_vigor_curva](#goldpredicoes_vigor_curva), [risco_climatico_fazenda](#goldrisco_climatico_fazenda), [risco_climatico_talhao](#goldrisco_climatico_talhao), [vigor_diario](#goldvigor_diario)
- **analytics** (Postgres) — [tabelas publicadas](#postgres-analytics)

## Bronze

Dados brutos, só acrescentados, com linhagem (`_ingest_ts`, `_source`).

### bronze.cad_culturas

Cadastro bruto de culturas (snapshot).

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `ciclo_medio_dias` | bigint | Ciclo médio da cultura (dias) |
| `descricao` | string | Descrição livre |
| `nome` | string | Nome do registro (fazenda, talhão, produtor…) |
| `temperatura_max` | bigint | Limite superior da faixa ideal de temperatura da cultura (°C) |
| `temperatura_min` | bigint | Limite inferior da faixa ideal de temperatura da cultura (°C) |
| `umidade_max` | bigint | Limite superior da faixa ideal de umidade (%) — em culturas; máxima do dia nas tabelas diárias |
| `umidade_min` | bigint | Limite inferior da faixa ideal de umidade (%) — em culturas; mínima do dia nas tabelas diárias |
| `variedades` | string | Variedades cadastradas para a cultura |
| `_arquivo` | string | Arquivo de origem no Volume landing |
| `_ingest_ts` | timestamp | Momento em que a linha entrou na Bronze (UTC) |
| `_source` | string | Origem do dado: open_meteo, cadastro_frontend, simulador_iot, registro_produtor_simulado, sentinel2_simulado, extrator_externo |

### bronze.cad_fazendas

Cadastro bruto de fazendas (snapshot).

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `area_total_ha` | bigint | Área total da fazenda (ha) |
| `id` | bigint | Identificador no cadastro de origem |
| `latitude` | double | Latitude (graus decimais) |
| `longitude` | double | Longitude (graus decimais) |
| `municipio` | string | Município |
| `nome` | string | Nome do registro (fazenda, talhão, produtor…) |
| `produtor` | string | Produtor responsável |
| `status` | string | Situação do registro (ativo, em_andamento, concluida…) |
| `uf` | string | Estado (UF) |
| `_arquivo` | string | Arquivo de origem no Volume landing |
| `_ingest_ts` | timestamp | Momento em que a linha entrou na Bronze (UTC) |
| `_source` | string | Origem do dado: open_meteo, cadastro_frontend, simulador_iot, registro_produtor_simulado, sentinel2_simulado, extrator_externo |

### bronze.cad_logistica

Cadastro bruto de logistica (snapshot).

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `custo` | string | Custo estimado de transporte |
| `destino` | string | Destino da carga (logística) |
| `modal` | string | Modal de transporte |
| `origem` | string | Origem: archive (reanálise) ou forecast no clima; local de partida na logística |
| `sit` | string | Situação do transporte |
| `tempo` | string | Tempo estimado de transporte |
| `transp` | string | Transportadora |
| `_arquivo` | string | Arquivo de origem no Volume landing |
| `_ingest_ts` | timestamp | Momento em que a linha entrou na Bronze (UTC) |
| `_source` | string | Origem do dado: open_meteo, cadastro_frontend, simulador_iot, registro_produtor_simulado, sentinel2_simulado, extrator_externo |

### bronze.cad_mercado

Cadastro bruto de mercado (snapshot).

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `cultura` | string | Cultura: Manga, Uva, Goiaba ou Melão |
| `data` | string | Data (dia local de Brasília nas tabelas diárias) |
| `demanda` | string | Demanda estimada |
| `mercado` | string | Mercado de destino (cadastro de mercado) |
| `moeda` | string | Moeda do preço |
| `preco` | string | Preço médio de referência |
| `_arquivo` | string | Arquivo de origem no Volume landing |
| `_ingest_ts` | timestamp | Momento em que a linha entrou na Bronze (UTC) |
| `_source` | string | Origem do dado: open_meteo, cadastro_frontend, simulador_iot, registro_produtor_simulado, sentinel2_simulado, extrator_externo |

### bronze.cad_produtores

Cadastro bruto de produtores (snapshot).

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `cidade` | string | Cidade |
| `doc` | string | CPF/CNPJ (cadastro de produtores) |
| `email` | string | E-mail de contato |
| `nome` | string | Nome do registro (fazenda, talhão, produtor…) |
| `resp` | string | Responsável |
| `tel` | string | Telefone |
| `uf` | string | Estado (UF) |
| `_arquivo` | string | Arquivo de origem no Volume landing |
| `_ingest_ts` | timestamp | Momento em que a linha entrou na Bronze (UTC) |
| `_source` | string | Origem do dado: open_meteo, cadastro_frontend, simulador_iot, registro_produtor_simulado, sentinel2_simulado, extrator_externo |

### bronze.cad_sensores

Cadastro bruto de sensores (snapshot).

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `bateria` | bigint | Bateria do sensor (%) |
| `codigo` | string | Código do talhão (T-01, BV-03…) ou do sensor (SN-101…), conforme a tabela |
| `fazenda_id` | bigint | Fazenda: 1 Santa Helena (Petrolina-PE), 2 Boa Vista (Juazeiro-BA), 3 Três Irmãos (Mossoró-RN) |
| `firmware` | string | Versão de firmware do sensor |
| `latitude` | double | Latitude (graus decimais) |
| `longitude` | double | Longitude (graus decimais) |
| `offline_atual` | boolean | Sensor offline no momento do cadastro |
| `sinal_dbm` | bigint | Intensidade do sinal (dBm) |
| `talhao_codigo` | string | Código do talhão — chave de junção entre todas as camadas e com o backend |
| `tipo` | string | Tipo de sensor: estacao_climatica ou sonda_solo |
| `_arquivo` | string | Arquivo de origem no Volume landing |
| `_ingest_ts` | timestamp | Momento em que a linha entrou na Bronze (UTC) |
| `_source` | string | Origem do dado: open_meteo, cadastro_frontend, simulador_iot, registro_produtor_simulado, sentinel2_simulado, extrator_externo |

### bronze.cad_talhoes

Cadastro bruto de talhoes (snapshot).

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `codigo` | string | Código do talhão (T-01, BV-03…) ou do sensor (SN-101…), conforme a tabela |
| `cultura` | string | Cultura: Manga, Uva, Goiaba ou Melão |
| `data_plantio_atual` | string | Data de plantio/poda da safra atual (cadastro) |
| `estagio` | string | Estágio fenológico atual: poda_inducao, floracao, frutificacao, maturacao, colheita |
| `fazenda_id` | bigint | Fazenda: 1 Santa Helena (Petrolina-PE), 2 Boa Vista (Juazeiro-BA), 3 Três Irmãos (Mossoró-RN) |
| `nome` | string | Nome do registro (fazenda, talhão, produtor…) |
| `ph_referencia` | double | pH de referência do solo do talhão |
| `poligono` | string | Contorno do talhão em GeoJSON (Polygon, longitude/latitude) |
| `previsao_colheita_atual` | string | Previsão de colheita informada no cadastro |
| `variedade` | string | Variedade da cultura (Palmer, Vitória, Paluma…) |
| `_arquivo` | string | Arquivo de origem no Volume landing |
| `_ingest_ts` | timestamp | Momento em que a linha entrou na Bronze (UTC) |
| `_source` | string | Origem do dado: open_meteo, cadastro_frontend, simulador_iot, registro_produtor_simulado, sentinel2_simulado, extrator_externo |

### bronze.clima_openmeteo_horario

Clima horário Open-Meteo (archive ERA5 + forecast), bruto, append-only, UTC.

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `fazenda_id` | int | Fazenda: 1 Santa Helena (Petrolina-PE), 2 Boa Vista (Juazeiro-BA), 3 Três Irmãos (Mossoró-RN) |
| `origem` | string | Origem: archive (reanálise) ou forecast no clima; local de partida na logística |
| `time` | string | Hora UTC como veio da API (texto) |
| `temperature_2m` | double | Temperatura do ar a 2 m (°C) |
| `relative_humidity_2m` | double | Umidade relativa do ar a 2 m (%) |
| `precipitation` | double | Chuva na hora (mm) |
| `soil_moisture_0_to_7cm` | double | Umidade volumétrica do solo, 0–7 cm (m³/m³) |
| `soil_moisture_7_to_28cm` | double | Umidade volumétrica do solo, 7–28 cm (m³/m³) |
| `shortwave_radiation` | double | Radiação solar de onda curta (W/m²) |
| `et0_fao_evapotranspiration` | double | Evapotranspiração de referência FAO (mm) |
| `wind_speed_10m` | double | Vento a 10 m (km/h) |
| `_ingest_ts` | timestamp | Momento em que a linha entrou na Bronze (UTC) |
| `_source` | string | Origem do dado: open_meteo, cadastro_frontend, simulador_iot, registro_produtor_simulado, sentinel2_simulado, extrator_externo |

### bronze.clima_previsao_diaria

Previsão diária Open-Meteo (16 dias), um snapshot por execução.

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `fazenda_id` | int | Fazenda: 1 Santa Helena (Petrolina-PE), 2 Boa Vista (Juazeiro-BA), 3 Três Irmãos (Mossoró-RN) |
| `data` | string | Data (dia local de Brasília nas tabelas diárias) |
| `emitida_em` | string | Quando o boletim de previsão foi baixado (UTC) |
| `temperature_2m_max` | double | Máxima prevista do dia (°C) |
| `temperature_2m_min` | double | Mínima prevista do dia (°C) |
| `precipitation_sum` | double | Chuva prevista no dia (mm) |
| `precipitation_probability_max` | double | Probabilidade máxima de chuva no dia (%) |
| `et0_fao_evapotranspiration` | double | Evapotranspiração de referência FAO (mm) |
| `_ingest_ts` | timestamp | Momento em que a linha entrou na Bronze (UTC) |
| `_source` | string | Origem do dado: open_meteo, cadastro_frontend, simulador_iot, registro_produtor_simulado, sentinel2_simulado, extrator_externo |

### bronze.leituras_iot

Mensagens IoT brutas (payload JSON do ESP32). Backfill simulado 2019→agora com sujeira proposital.

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `payload` | string | Mensagem bruta do sensor (JSON), como recebida |
| `received_at` | timestamp | Quando o gateway recebeu a mensagem (UTC) |
| `gateway` | string | Gateway ou origem da mensagem (GW-F1, backend-render, thingspeak) |
| `_ingest_ts` | timestamp | Momento em que a linha entrou na Bronze (UTC) |
| `_source` | string | Origem do dado: open_meteo, cadastro_frontend, simulador_iot, registro_produtor_simulado, sentinel2_simulado, extrator_externo |

### bronze.leituras_iot_externo

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `payload` | string | Mensagem bruta do sensor (JSON), como recebida |
| `received_at` | timestamp | Quando o gateway recebeu a mensagem (UTC) |
| `gateway` | string | Gateway ou origem da mensagem (GW-F1, backend-render, thingspeak) |
| `_ingest_ts` | timestamp | Momento em que a linha entrou na Bronze (UTC) |
| `_source` | string | Origem do dado: open_meteo, cadastro_frontend, simulador_iot, registro_produtor_simulado, sentinel2_simulado, extrator_externo |
| `_arquivo` | string | Arquivo de origem no Volume landing |

### bronze.mercado_precos_diarios

Preço diário por cultura e praça (R$/kg), simulado; praças de exportação ancoradas na ComexStat quando disponível.

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `cultura` | string | Cultura: Manga, Uva, Goiaba ou Melão |
| `praca` | string | Praça de venda: Petrolina (PE), CEAGESP (SP), Roterdã (Países Baixos), Filadélfia (EUA), Londres (Reino Unido) |
| `data` | date | Data (dia local de Brasília nas tabelas diárias) |
| `preco_brl_kg` | double | Preço do dia (R$/kg) |
| `usd_brl` | double | Dólar PTAX de venda (R$) |
| `origem_preco` | string | simulado ou simulado_ancorado_comexstat (forma mensal do preço real de exportação) |
| `origem_cambio` | string | real (PTAX do Banco Central) ou aproximado (média anual) |
| `_ingest_ts` | timestamp | Momento em que a linha entrou na Bronze (UTC) |
| `_source` | string | Origem do dado: open_meteo, cadastro_frontend, simulador_iot, registro_produtor_simulado, sentinel2_simulado, extrator_externo |

### bronze.safras_historico

Histórico de safras por talhão (simulado, modelo GDD + estresse).

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `talhao_codigo` | string | Código do talhão — chave de junção entre todas as camadas e com o backend |
| `safra_seq` | bigint | Número da safra no talhão, em ordem cronológica |
| `cultura` | string | Cultura: Manga, Uva, Goiaba ou Melão |
| `variedade` | string | Variedade da cultura (Palmer, Vitória, Paluma…) |
| `data_plantio` | date | Data de plantio/poda da safra |
| `status` | string | Situação do registro (ativo, em_andamento, concluida…) |
| `data_inicio_colheita` | date | Início real da colheita |
| `data_fim_colheita` | date | Fim real da colheita |
| `produtividade_t_ha` | double | Produtividade da safra (t/ha) |
| `destino_exportacao` | string | Destino da exportação (Roterdã, Filadélfia, Londres, CEAGESP, Recife) |
| `data_inicio_exportacao` | date | Início real da janela de exportação |
| `data_fim_exportacao` | date | Fim real da janela de exportação |
| `_ingest_ts` | timestamp | Momento em que a linha entrou na Bronze (UTC) |
| `_source` | string | Origem do dado: open_meteo, cadastro_frontend, simulador_iot, registro_produtor_simulado, sentinel2_simulado, extrator_externo |

### bronze.vigor_satelite

Índice de vigor (NDVI) por talhão a cada 5 dias, com falhas por nuvem (simulado).

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `talhao_codigo` | string | Código do talhão — chave de junção entre todas as camadas e com o backend |
| `data_imagem` | date | Data da imagem de satélite |
| `cobertura_nuvens_pct` | double | Cobertura de nuvens na imagem (%) |
| `ndvi` | double | Índice de vegetação (0 a 1) — vazio quando a imagem tinha nuvem |
| `_ingest_ts` | timestamp | Momento em que a linha entrou na Bronze (UTC) |
| `_source` | string | Origem do dado: open_meteo, cadastro_frontend, simulador_iot, registro_produtor_simulado, sentinel2_simulado, extrator_externo |

## Silver

Dados limpos: tipos corretos, UTC, sem duplicados, inválidos e extremos tratados.

### silver.clima_horario

Clima de referência por fazenda (Open-Meteo), 1 linha por hora UTC.

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `fazenda_id` | int | Fazenda: 1 Santa Helena (Petrolina-PE), 2 Boa Vista (Juazeiro-BA), 3 Três Irmãos (Mossoró-RN) |
| `ts_utc` | timestamp | Instante da leitura em UTC (já padronizado) |
| `origem` | string | Origem: archive (reanálise) ou forecast no clima; local de partida na logística |
| `temperatura` | double | Temperatura do ar (°C) |
| `umidade` | double | Umidade relativa do ar (%) |
| `chuva_mm` | double | Chuva (mm) — na hora ou somada no dia |
| `umidade_solo_m3m3` | double | Umidade volumétrica do solo do Open-Meteo (m³/m³) |
| `radiacao_wm2` | double | Radiação solar (W/m²) |
| `et0_mm` | double | Evapotranspiração de referência (mm) |
| `vento_kmh` | double | Vento (km/h) |

### silver.culturas

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `cultura` | string | Cultura: Manga, Uva, Goiaba ou Melão |
| `variedades` | string | Variedades cadastradas para a cultura |
| `temperatura_min` | double | Limite inferior da faixa ideal de temperatura da cultura (°C) |
| `temperatura_max` | double | Limite superior da faixa ideal de temperatura da cultura (°C) |
| `umidade_min` | double | Limite inferior da faixa ideal de umidade (%) — em culturas; mínima do dia nas tabelas diárias |
| `umidade_max` | double | Limite superior da faixa ideal de umidade (%) — em culturas; máxima do dia nas tabelas diárias |
| `ciclo_medio_dias` | int | Ciclo médio da cultura (dias) |
| `descricao` | string | Descrição livre |

### silver.fazendas

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `fazenda_id` | int | Fazenda: 1 Santa Helena (Petrolina-PE), 2 Boa Vista (Juazeiro-BA), 3 Três Irmãos (Mossoró-RN) |
| `nome` | string | Nome do registro (fazenda, talhão, produtor…) |
| `municipio` | string | Município |
| `uf` | string | Estado (UF) |
| `area_total_ha` | double | Área total da fazenda (ha) |
| `produtor` | string | Produtor responsável |
| `latitude` | double | Latitude (graus decimais) |
| `longitude` | double | Longitude (graus decimais) |

### silver.leituras

Leituras IoT tratadas: UTC, sem duplicados, inválidos/extremos anulados com motivo.

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `sensor_codigo` | string | Código do sensor (SN-101…) |
| `talhao_codigo` | string | Código do talhão — chave de junção entre todas as camadas e com o backend |
| `fazenda_id` | int | Fazenda: 1 Santa Helena (Petrolina-PE), 2 Boa Vista (Juazeiro-BA), 3 Três Irmãos (Mossoró-RN) |
| `tipo` | string | Tipo de sensor: estacao_climatica ou sonda_solo |
| `ts_utc` | timestamp | Instante da leitura em UTC (já padronizado) |
| `hora_utc` | timestamp | Hora cheia UTC (reamostragem) |
| `formato_data` | string | Formato original da data: iso_utc, iso_offset, epoch, local_sem_fuso |
| `received_at` | timestamp | Quando o gateway recebeu a mensagem (UTC) |
| `temperatura` | double | Temperatura do ar (°C) |
| `umidade` | double | Umidade relativa do ar (%) |
| `precipitacao` | double | Chuva registrada pelo sensor (mm) |
| `luminosidade` | double | Luminosidade (lux) |
| `umidade_solo` | double | Umidade do solo (%) |
| `ph` | double | pH do solo |
| `temperatura_solo` | double | Temperatura do solo (°C) |
| `bateria` | double | Bateria do sensor (%) |
| `rssi` | double | Intensidade do sinal na mensagem (dBm) |
| `problemas` | array<string> | Lista de problemas tratados na leitura (valor inválido, extremo, campo ausente) |

### silver.leituras_horarias

Leituras reamostradas por talhão e hora (UTC).

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `talhao_codigo` | string | Código do talhão — chave de junção entre todas as camadas e com o backend |
| `fazenda_id` | int | Fazenda: 1 Santa Helena (Petrolina-PE), 2 Boa Vista (Juazeiro-BA), 3 Três Irmãos (Mossoró-RN) |
| `hora_utc` | timestamp | Hora cheia UTC (reamostragem) |
| `temperatura` | double | Temperatura do ar (°C) |
| `umidade` | double | Umidade relativa do ar (%) |
| `umidade_solo` | double | Umidade do solo (%) |
| `ph` | double | pH do solo |
| `luminosidade` | double | Luminosidade (lux) |
| `temperatura_solo` | double | Temperatura do solo (°C) |
| `precipitacao` | double | Chuva registrada pelo sensor (mm) |
| `sensores_ativos` | bigint | Quantos sensores do talhão enviaram leitura na hora |

### silver.leituras_quarentena

Mensagens rejeitadas com motivo.

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `payload` | string | Mensagem bruta do sensor (JSON), como recebida |
| `received_at` | timestamp | Quando o gateway recebeu a mensagem (UTC) |
| `gateway` | string | Gateway ou origem da mensagem (GW-F1, backend-render, thingspeak) |
| `motivo_quarentena` | string | Por que a mensagem foi rejeitada (sensor_nao_cadastrado, data_ilegivel…) |

### silver.mercado_fretes

Frete estimado por kg até cada praça (parâmetro ilustrativo).

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `praca` | string | Praça de venda: Petrolina (PE), CEAGESP (SP), Roterdã (Países Baixos), Filadélfia (EUA), Londres (Reino Unido) |
| `frete_brl_kg` | double | Frete estimado até a praça (R$/kg, parâmetro ilustrativo) |
| `rota` | string | Rota de transporte considerada |
| `origem` | string | Origem: archive (reanálise) ou forecast no clima; local de partida na logística |

### silver.mercado_precos

Preço diário por cultura e praça (R$/kg), validado (> 0, 1 por dia).

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `cultura` | string | Cultura: Manga, Uva, Goiaba ou Melão |
| `praca` | string | Praça de venda: Petrolina (PE), CEAGESP (SP), Roterdã (Países Baixos), Filadélfia (EUA), Londres (Reino Unido) |
| `data` | date | Data (dia local de Brasília nas tabelas diárias) |
| `preco_brl_kg` | double | Preço do dia (R$/kg) |
| `usd_brl` | double | Dólar PTAX de venda (R$) |
| `origem_preco` | string | simulado ou simulado_ancorado_comexstat (forma mensal do preço real de exportação) |
| `origem_cambio` | string | real (PTAX do Banco Central) ou aproximado (média anual) |

### silver.previsao_diaria

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `fazenda_id` | int | Fazenda: 1 Santa Helena (Petrolina-PE), 2 Boa Vista (Juazeiro-BA), 3 Três Irmãos (Mossoró-RN) |
| `data` | date | Data (dia local de Brasília nas tabelas diárias) |
| `emitida_em` | string | Quando o boletim de previsão foi baixado (UTC) |
| `temp_max` | double | Temperatura máxima (°C) — do dia medida ou prevista, conforme a tabela |
| `temp_min` | double | Temperatura mínima (°C) — do dia medida ou prevista, conforme a tabela |
| `chuva_prevista_mm` | double | Chuva prevista no dia (mm) |
| `prob_chuva_pct` | double | Probabilidade de chuva (%) |
| `et0_mm` | double | Evapotranspiração de referência (mm) |

### silver.qualidade_leituras

Contagem de problemas tratados na última carga.

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `problema` | string | Tipo de problema tratado na carga |
| `count` | bigint | Quantidade de ocorrências |
| `total_mensagens` | int | Total de mensagens da carga |
| `gerado_em` | timestamp | Quando foi gerado (UTC) |

### silver.safras

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `talhao_codigo` | string | Código do talhão — chave de junção entre todas as camadas e com o backend |
| `safra_seq` | int | Número da safra no talhão, em ordem cronológica |
| `cultura` | string | Cultura: Manga, Uva, Goiaba ou Melão |
| `variedade` | string | Variedade da cultura (Palmer, Vitória, Paluma…) |
| `status` | string | Situação do registro (ativo, em_andamento, concluida…) |
| `data_plantio` | date | Data de plantio/poda da safra |
| `data_inicio_colheita` | date | Início real da colheita |
| `data_fim_colheita` | date | Fim real da colheita |
| `data_inicio_exportacao` | date | Início real da janela de exportação |
| `data_fim_exportacao` | date | Fim real da janela de exportação |
| `produtividade_t_ha` | double | Produtividade da safra (t/ha) |
| `destino_exportacao` | string | Destino da exportação (Roterdã, Filadélfia, Londres, CEAGESP, Recife) |
| `safra_id` | string | Safra: talhão + sequência, ex. T-01-07 |
| `dias_ate_colheita` | int | Dias entre plantio e início da colheita |
| `duracao_colheita` | int | Duração da colheita (dias) |
| `produtividade_valida` | boolean | Produtividade dentro da faixa plausível (1–120 t/ha) ou vazia |

### silver.sensores

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `sensor_codigo` | string | Código do sensor (SN-101…) |
| `talhao_codigo` | string | Código do talhão — chave de junção entre todas as camadas e com o backend |
| `fazenda_id` | int | Fazenda: 1 Santa Helena (Petrolina-PE), 2 Boa Vista (Juazeiro-BA), 3 Três Irmãos (Mossoró-RN) |
| `tipo` | string | Tipo de sensor: estacao_climatica ou sonda_solo |
| `latitude` | double | Latitude (graus decimais) |
| `longitude` | double | Longitude (graus decimais) |
| `offline_atual` | boolean | Sensor offline no momento do cadastro |
| `bateria` | int | Bateria do sensor (%) |
| `sinal_dbm` | int | Intensidade do sinal (dBm) |
| `firmware` | string | Versão de firmware do sensor |

### silver.talhoes

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `talhao_codigo` | string | Código do talhão — chave de junção entre todas as camadas e com o backend |
| `fazenda_id` | int | Fazenda: 1 Santa Helena (Petrolina-PE), 2 Boa Vista (Juazeiro-BA), 3 Três Irmãos (Mossoró-RN) |
| `nome` | string | Nome do registro (fazenda, talhão, produtor…) |
| `cultura` | string | Cultura: Manga, Uva, Goiaba ou Melão |
| `variedade` | string | Variedade da cultura (Palmer, Vitória, Paluma…) |
| `data_plantio_atual` | date | Data de plantio/poda da safra atual (cadastro) |
| `previsao_colheita_cadastro` | date | Previsão de colheita informada no cadastro |
| `estagio` | string | Estágio fenológico atual: poda_inducao, floracao, frutificacao, maturacao, colheita |
| `poligono` | string | Contorno do talhão em GeoJSON (Polygon, longitude/latitude) |
| `ph_referencia` | double | pH de referência do solo do talhão |

### silver.vigor

Índice de vigor válido (NDVI, nuvem <= 50%).

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `talhao_codigo` | string | Código do talhão — chave de junção entre todas as camadas e com o backend |
| `data` | date | Data (dia local de Brasília nas tabelas diárias) |
| `indice_vigor` | double | Índice de vigor (NDVI) de uma imagem válida |
| `cobertura_nuvens_pct` | double | Cobertura de nuvens na imagem (%) |

## Gold

Agregados por talhão, indicadores, features e previsões consumidas pela aplicação.

### gold.clima_diario_talhao

Agregado diário por talhão (dia local), com faixas da cultura e graus-dia.

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `fazenda_id` | int | Fazenda: 1 Santa Helena (Petrolina-PE), 2 Boa Vista (Juazeiro-BA), 3 Três Irmãos (Mossoró-RN) |
| `data` | date | Data (dia local de Brasília nas tabelas diárias) |
| `talhao_codigo` | string | Código do talhão — chave de junção entre todas as camadas e com o backend |
| `cultura` | string | Cultura: Manga, Uva, Goiaba ou Melão |
| `temp_media` | double | Temperatura média do dia (°C) |
| `temp_max` | double | Temperatura máxima (°C) — do dia medida ou prevista, conforme a tabela |
| `temp_min` | double | Temperatura mínima (°C) — do dia medida ou prevista, conforme a tabela |
| `umidade_media` | double | Umidade média do dia (%) |
| `umidade_min` | double | Limite inferior da faixa ideal de umidade (%) — em culturas; mínima do dia nas tabelas diárias |
| `umidade_max` | double | Limite superior da faixa ideal de umidade (%) — em culturas; máxima do dia nas tabelas diárias |
| `umidade_solo_media` | double | Umidade do solo média (%) — no dia ou desde o plantio, conforme a tabela |
| `ph_medio` | double | pH médio do dia |
| `chuva_mm` | double | Chuva (mm) — na hora ou somada no dia |
| `horas_temp_fora_faixa` | bigint | Horas do dia com temperatura fora da faixa ideal da cultura |
| `horas_umid_fora_faixa` | bigint | Horas do dia com umidade fora da faixa ideal da cultura |
| `horas_condicao_adequada` | bigint | Horas do dia com temperatura e umidade dentro da faixa |
| `horas_acima_tmax` | bigint | Horas do dia acima do teto de temperatura da cultura |
| `horas_com_leitura` | bigint | Horas do dia com leitura válida de temperatura |
| `chuva_ref` | double | Chuva do dia pelo Open-Meteo (mm), usada quando falta sensor |
| `et0_ref` | double | Evapotranspiração do dia pelo Open-Meteo (mm) |
| `origem_temp` | string | De onde veio a temperatura do dia: sensor ou referencia_openmeteo |
| `graus_dia` | double | Graus-dia do dia: média limitada a 36 °C menos a temperatura base da cultura |
| `completude_pct` | double | % das 24 horas com leitura válida |

### gold.features_safra

Features por talhão × safra × ponto de referência para as regressões (treino e previsão).

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `talhao_codigo` | string | Código do talhão — chave de junção entre todas as camadas e com o backend |
| `safra_id` | string | Safra: talhão + sequência, ex. T-01-07 |
| `cultura` | string | Cultura: Manga, Uva, Goiaba ou Melão |
| `variedade` | string | Variedade da cultura (Palmer, Vitória, Paluma…) |
| `status` | string | Situação do registro (ativo, em_andamento, concluida…) |
| `data_referencia` | date | Data de referência |
| `data_plantio` | date | Data de plantio/poda da safra |
| `dias_desde_plantio` | double | Dias desde o plantio/poda até a data de referência |
| `fracao_ciclo` | double | Dias desde o plantio ÷ ciclo nominal da cultura |
| `umidade_solo_media` | double | Umidade do solo média (%) — no dia ou desde o plantio, conforme a tabela |
| `umidade_solo_30d` | double | Umidade do solo média dos 30 dias anteriores à referência (%) |
| `temp_media` | double | Temperatura média do dia (°C) |
| `temp_media_30d` | double | Temperatura média dos 30 dias anteriores à referência (°C) |
| `graus_dia_acum` | double | Graus-dia acumulados desde o plantio |
| `frac_horas_acima_tmax` | double | Fração das horas desde o plantio acima do teto de temperatura da cultura |
| `chuva_prevista_14d` | double | Chuva nos 14 dias seguintes (mm): prevista hoje; observada nas linhas históricas |
| `chuva_acum_30d` | double | Chuva dos 30 dias anteriores à referência (mm) |
| `produtividade_safra_anterior` | double | Produtividade da safra anterior do talhão (t/ha) |
| `produtividade_media_talhao` | double | Média de produtividade das safras anteriores do talhão (t/ha) |
| `dias_colheita_safra_anterior` | double | Dias até a colheita na safra anterior |
| `vigor_atual` | double | Último índice de vigor até a referência |
| `vigor_max` | double | Maior vigor da safra até a referência |
| `vigor_tendencia_30d` | double | Variação do vigor em 30 dias (inclinação da reta) |
| `produtividade_t_ha` | double | Produtividade da safra (t/ha) |
| `dias_ate_inicio_colheita` | double | ALVO: dias entre a referência e o início da colheita |
| `duracao_colheita` | double | Duração da colheita (dias) |
| `offset_exportacao` | double | ALVO: dias entre o início da colheita e o início da exportação |
| `duracao_exportacao` | double | ALVO: duração da janela de exportação (dias) |
| `destino_exportacao` | string | Destino da exportação (Roterdã, Filadélfia, Londres, CEAGESP, Recife) |
| `conjunto` | string | treino (safra concluída) ou prever (safra atual) |

### gold.indicadores_talhao

Indicadores de produção e colheita por talhão (snapshot atual).

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `talhao_codigo` | string | Código do talhão — chave de junção entre todas as camadas e com o backend |
| `fazenda_id` | int | Fazenda: 1 Santa Helena (Petrolina-PE), 2 Boa Vista (Juazeiro-BA), 3 Três Irmãos (Mossoró-RN) |
| `nome` | string | Nome do registro (fazenda, talhão, produtor…) |
| `cultura` | string | Cultura: Manga, Uva, Goiaba ou Melão |
| `variedade` | string | Variedade da cultura (Palmer, Vitória, Paluma…) |
| `previsao_colheita_cadastro` | date | Previsão de colheita informada no cadastro |
| `indice_condicoes_adequadas_7d` | double | % das horas dos últimos 7 dias com temperatura e umidade na faixa |
| `horas_temp_fora_faixa_7d` | bigint | Horas com temperatura fora da faixa nos últimos 7 dias |
| `horas_umid_fora_faixa_7d` | bigint | Horas com umidade fora da faixa nos últimos 7 dias |
| `temp_media_7d` | double | Temperatura média dos últimos 7 dias (°C) |
| `umidade_solo_7d` | double | Umidade do solo média dos últimos 7 dias (%) |
| `data_plantio` | date | Data de plantio/poda da safra |
| `evolucao_condicoes_pp` | double | Variação do índice de condições adequadas vs os 7 dias anteriores (pontos percentuais) |
| `tendencia` | string | melhorando, estavel ou piorando |
| `dias_desde_plantio` | int | Dias desde o plantio/poda até a data de referência |
| `dias_ate_colheita_cadastro` | int | Dias até a colheita prevista no cadastro |
| `atualizado_em` | timestamp | Quando o indicador foi calculado (UTC) |

### gold.mercado_modelo_metadados

Métricas da validação temporal (180 dias) do modelo de preço e comparação com regras simples.

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `modelo` | string | Alvo do modelo |
| `horizonte_dias` | bigint | Horizonte da previsão (dias) |
| `mape` | double | Erro percentual médio no teste temporal (%) |
| `mape_ingenuo` | double | Erro percentual médio da regra 'o preço fica igual' (%) |
| `mape_sazonal` | double | Erro percentual médio da regra 'repete a variação de um ano antes' (%) |
| `mae_brl` | double | Erro absoluto médio (R$/kg) |
| `mae_brl_ingenuo` | double | Erro absoluto médio da regra 'o preço fica igual' (R$/kg) |
| `acerto_direcao` | double | Fração dos casos em que o modelo acertou se o preço sobe ou desce |
| `cobertura_90` | double | Fração dos casos de teste dentro do intervalo de 90% |
| `n_treino` | bigint | Linhas de treino |
| `n_teste` | bigint | Linhas de teste |
| `mlflow_run_id` | string | Execução do MLflow |
| `treinado_em` | string | Quando o modelo foi treinado (UTC) |

### gold.mercado_previsao

Previsão de preço por cultura e praça em 7, 14 e 30 dias, com intervalo de 90%.

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `cultura` | string | Cultura: Manga, Uva, Goiaba ou Melão |
| `praca` | string | Praça de venda: Petrolina (PE), CEAGESP (SP), Roterdã (Países Baixos), Filadélfia (EUA), Londres (Reino Unido) |
| `horizonte_dias` | bigint | Horizonte da previsão (dias) |
| `data_base` | date | Último dia com preço usado na previsão |
| `data_alvo` | date | Dia previsto |
| `preco_atual` | double | Último preço da série (R$/kg) |
| `preco_previsto` | double | Preço previsto (R$/kg), mediana |
| `preco_p05` | double | Limite inferior do intervalo de 90% (R$/kg) |
| `preco_p95` | double | Limite superior do intervalo de 90% (R$/kg) |
| `variacao_prevista_pct` | double | Variação prevista sobre o preço atual (%) |
| `modelo_run_id` | string | Execução do MLflow que gerou a previsão |
| `gerado_em` | string | Quando foi gerado (UTC) |

### gold.mercado_referencias

Referências por praça: preço, variação, mínima/máxima 90 dias, frete, líquido e melhor destino.

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `cultura` | string | Cultura: Manga, Uva, Goiaba ou Melão |
| `praca` | string | Praça de venda: Petrolina (PE), CEAGESP (SP), Roterdã (Países Baixos), Filadélfia (EUA), Londres (Reino Unido) |
| `data` | date | Data (dia local de Brasília nas tabelas diárias) |
| `preco_atual` | double | Último preço da série (R$/kg) |
| `variacao_30d_pct` | double | Variação do preço em 30 dias (%) |
| `variacao_90d_pct` | double | Variação do preço em 90 dias (%) |
| `minima_90d` | double | Menor preço nos últimos 90 dias (R$/kg) |
| `maxima_90d` | double | Maior preço nos últimos 90 dias (R$/kg) |
| `frete_brl_kg` | double | Frete estimado até a praça (R$/kg, parâmetro ilustrativo) |
| `liquido_brl_kg` | double | Preço menos frete (R$/kg) |
| `preco_previsto_30d` | double | Preço previsto em 30 dias (R$/kg) |
| `liquido_previsto_30d` | double | Preço previsto em 30 dias menos frete (R$/kg) |
| `melhor_liquido` | boolean | Praça com o maior líquido hoje para a cultura |
| `melhor_liquido_previsto_30d` | boolean | Praça com o maior líquido previsto em 30 dias |
| `origem_preco` | string | simulado ou simulado_ancorado_comexstat (forma mensal do preço real de exportação) |

### gold.mercado_serie

Preço diário por cultura e praça, últimos 365 dias (gráfico da tela Mercado).

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `cultura` | string | Cultura: Manga, Uva, Goiaba ou Melão |
| `praca` | string | Praça de venda: Petrolina (PE), CEAGESP (SP), Roterdã (Países Baixos), Filadélfia (EUA), Londres (Reino Unido) |
| `data` | date | Data (dia local de Brasília nas tabelas diárias) |
| `preco_brl_kg` | double | Preço do dia (R$/kg) |
| `origem_preco` | string | simulado ou simulado_ancorado_comexstat (forma mensal do preço real de exportação) |

### gold.modelo_fatores

Importância (permutação agrupada) do modelo de produtividade.

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `fator` | string | Grupo de variáveis do modelo de produtividade |
| `peso_pct` | double | Peso do grupo no modelo (% da importância por permutação) |

### gold.modelo_metadados

Métricas da validação temporal e metadados do treino.

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `modelo` | string | Alvo do modelo |
| `descricao` | string | Descrição livre |
| `estrategia` | string | ml ou media_por_cultura (quando a regressão não superou a média) |
| `mae` | double | Erro absoluto médio no teste temporal (unidade do alvo) |
| `r2` | double | R² no teste temporal |
| `cobertura_intervalo_90_bruta` | double | A mesma fração antes da calibração conformal |
| `mae_baseline` | double | Erro absoluto médio usando só a média da cultura |
| `cobertura_intervalo_90` | double | Fração dos casos de teste dentro do intervalo de 90% calibrado |
| `n_safras_treino` | bigint | Safras usadas no treino |
| `n_sensores` | bigint | Sensores cadastrados |
| `mlflow_run_id` | string | Execução do MLflow |
| `treinado_em` | string | Quando o modelo foi treinado (UTC) |

### gold.predicoes_talhao

Predição por talhão: produtividade, janela de colheita e de exportação (tela Predições de IA).

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `talhao_codigo` | string | Código do talhão — chave de junção entre todas as camadas e com o backend |
| `safra_id` | string | Safra: talhão + sequência, ex. T-01-07 |
| `cultura` | string | Cultura: Manga, Uva, Goiaba ou Melão |
| `variedade` | string | Variedade da cultura (Palmer, Vitória, Paluma…) |
| `data_referencia` | date | Data de referência |
| `produtividade_prevista` | double | Produtividade prevista (t/ha), mediana |
| `produtividade_p05` | double | Limite inferior do intervalo de 90% (t/ha) |
| `produtividade_p95` | double | Limite superior do intervalo de 90% (t/ha) |
| `produtividade_safra_anterior` | double | Produtividade da safra anterior do talhão (t/ha) |
| `variacao_safra_anterior_pct` | double | Variação da produtividade prevista sobre a safra anterior (%) |
| `colheita_inicio` | date | Início previsto da colheita |
| `colheita_fim` | date | Fim previsto da colheita |
| `confianca_colheita_pct` | double | Confiança na data de colheita (%): 99 = intervalo estreito; 50 = intervalo de 30 dias ou mais |
| `exportacao_inicio` | date | Início previsto da exportação |
| `exportacao_fim` | date | Fim previsto da exportação |
| `destino_exportacao` | string | Destino da exportação (Roterdã, Filadélfia, Londres, CEAGESP, Recife) |
| `motivo` | string | Explicação em texto dos fatores da previsão ou do risco |
| `vigor_atual` | double | Último índice de vigor até a referência |
| `fazenda_id` | bigint | Fazenda: 1 Santa Helena (Petrolina-PE), 2 Boa Vista (Juazeiro-BA), 3 Três Irmãos (Mossoró-RN) |
| `modelo_run_id` | string | Execução do MLflow que gerou a previsão |
| `gerado_em` | string | Quando foi gerado (UTC) |

### gold.predicoes_vigor_curva

Curva de vigor projetada com intervalo de 90%.

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `talhao_codigo` | string | Código do talhão — chave de junção entre todas as camadas e com o backend |
| `data` | date | Data (dia local de Brasília nas tabelas diárias) |
| `vigor_previsto` | double | Vigor previsto (mediana) |
| `vigor_p05` | double | Limite inferior do intervalo de 90% do vigor |
| `vigor_p95` | double | Limite superior do intervalo de 90% do vigor |

### gold.risco_climatico_fazenda

Risco climático 7 dias por fazenda (pior talhão).

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `fazenda_id` | int | Fazenda: 1 Santa Helena (Petrolina-PE), 2 Boa Vista (Juazeiro-BA), 3 Três Irmãos (Mossoró-RN) |
| `talhoes_em_risco` | string | Talhões da fazenda com risco Moderado ou Alto |
| `nivel` | string | Baixo (0–2), Moderado (3–7) ou Alto (≥ 8) |
| `resumo` | string | Resumo do risco da fazenda |
| `gerado_em` | timestamp | Quando foi gerado (UTC) |

### gold.risco_climatico_talhao

Risco climático 7 dias por talhão (regra sobre a previsão).

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `talhao_codigo` | string | Código do talhão — chave de junção entre todas as camadas e com o backend |
| `fazenda_id` | int | Fazenda: 1 Santa Helena (Petrolina-PE), 2 Boa Vista (Juazeiro-BA), 3 Três Irmãos (Mossoró-RN) |
| `nome` | string | Nome do registro (fazenda, talhão, produtor…) |
| `cultura` | string | Cultura: Manga, Uva, Goiaba ou Melão |
| `temperatura_max` | double | Limite superior da faixa ideal de temperatura da cultura (°C) |
| `colheita_inicio` | date | Início previsto da colheita |
| `dias_calor` | bigint | Dias de calor acima do normal nos próximos 7 dias |
| `dias_frio` | bigint | Dias com média abaixo do piso da cultura nos próximos 7 dias |
| `tmax_p90_historico` | double | Percentil 90 das máximas diárias da fazenda no mesmo mês, 2019→ (°C) |
| `temp_max_prevista` | double | Maior máxima prevista nos próximos 7 dias (°C) |
| `chuva_7d_mm` | double | Chuva prevista nos próximos 7 dias (mm) |
| `dias_chuva_forte` | bigint | Dias com chuva prevista ≥ 20 mm |
| `colheita_proxima` | boolean | Colheita prevista para os próximos 21 dias |
| `pontos` | bigint | Pontuação de risco: 2 por dia de calor ou frio + 3 por dia de chuva forte perto da colheita |
| `nivel` | string | Baixo (0–2), Moderado (3–7) ou Alto (≥ 8) |
| `motivo` | string | Explicação em texto dos fatores da previsão ou do risco |
| `gerado_em` | timestamp | Quando foi gerado (UTC) |

### gold.vigor_diario

Índice de vigor diário interpolado (lacunas <= 30 dias).

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `talhao_codigo` | string | Código do talhão — chave de junção entre todas as camadas e com o backend |
| `data` | date | Data (dia local de Brasília nas tabelas diárias) |
| `vigor` | double | Índice de vigor diário (interpolado entre imagens) |
| `observado` | boolean | Verdadeiro quando o vigor do dia veio de uma imagem, falso quando interpolado |

## Postgres `analytics`

Cópias das tabelas Gold gravadas no Postgres (Aiven) pelo extrator, com as mesmas colunas e tipos equivalentes. Recriadas por troca atômica a cada publicação.

| Tabela no Postgres | Origem no Databricks |
| --- | --- |
| `analytics.predicoes_talhao` | `campo_vivo.gold.predicoes_talhao` |
| `analytics.predicoes_vigor_curva` | `campo_vivo.gold.predicoes_vigor_curva` |
| `analytics.vigor_medido` | `campo_vivo.gold.vigor_diario (últimos 120 dias)` |
| `analytics.modelo_fatores` | `campo_vivo.gold.modelo_fatores` |
| `analytics.modelo_metadados` | `campo_vivo.gold.modelo_metadados` |
| `analytics.risco_climatico_talhao` | `campo_vivo.gold.risco_climatico_talhao` |
| `analytics.risco_climatico_fazenda` | `campo_vivo.gold.risco_climatico_fazenda` |
| `analytics.indicadores_talhao` | `campo_vivo.gold.indicadores_talhao` |
| `analytics.mercado_serie` | `campo_vivo.gold.mercado_serie` |
| `analytics.mercado_referencias` | `campo_vivo.gold.mercado_referencias` |
| `analytics.mercado_previsao` | `campo_vivo.gold.mercado_previsao` |
| `analytics.mercado_modelo_metadados` | `campo_vivo.gold.mercado_modelo_metadados` |
