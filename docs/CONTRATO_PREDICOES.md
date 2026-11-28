# Contrato `/predicoes` — para o backend (NestJS) e o frontend

**Para:** Tiago e Ruth · **De:** time de dados · **Atualizado:** 24/11/2026

O pipeline do Databricks publica as tabelas abaixo no **Postgres do Render**, no schema **`analytics`**
(separado do schema `public` do Prisma: `prisma migrate` não toca nelas). Elas são recriadas todo dia
por troca atômica, às ~06:00 BRT, pelo workflow `diario` do repositório `data-platform`.

O backend só precisa **ler** essas tabelas (`prisma.$queryRaw`) e expor os endpoints abaixo, protegidos por JWT.
A chave de junção com o resto do sistema é **`talhao_codigo`** (`T-01`, `BV-03`, `TI-02`…).

## Tabelas publicadas (`analytics.*`)

| Tabela | Grão | Colunas principais |
|---|---|---|
| `predicoes_talhao` | 1 linha por talhão (safra atual) | talhao_codigo, fazenda_id, cultura, variedade, produtividade_prevista, produtividade_p05, produtividade_p95, produtividade_safra_anterior, variacao_safra_anterior_pct, colheita_inicio, colheita_fim, confianca_colheita_pct, exportacao_inicio, exportacao_fim, destino_exportacao, motivo, vigor_atual, modelo_run_id, gerado_em |
| `predicoes_vigor_curva` | talhão × data futura (semanal) | talhao_codigo, data, vigor_previsto, vigor_p05, vigor_p95 |
| `vigor_medido` | talhão × dia (últimos 120 dias) | talhao_codigo, data, vigor, observado |
| `modelo_fatores` | 1 linha por fator | fator, peso_pct |
| `modelo_metadados` | 1 linha por modelo | modelo, descricao, estrategia, mae, r2, mae_baseline, cobertura_intervalo_90, n_safras_treino, n_sensores, mlflow_run_id, treinado_em |
| `risco_climatico_talhao` | 1 linha por talhão | talhao_codigo, fazenda_id, cultura, temp_max_prevista, tmax_p90_historico, chuva_7d_mm, dias_calor, dias_frio, pontos, nivel, motivo |
| `risco_climatico_fazenda` | 1 linha por fazenda | fazenda_id, nivel (Baixo/Moderado/Alto), talhoes_em_risco, resumo |
| `indicadores_talhao` | 1 linha por talhão | indice_condicoes_adequadas_7d, horas_temp_fora_faixa_7d, horas_umid_fora_faixa_7d, temp_media_7d, umidade_solo_7d, evolucao_condicoes_pp, tendencia, dias_desde_plantio, dias_ate_colheita_cadastro |

## Endpoints

Todos aceitam `?talhoes=T-01,T-02` (opcional) para filtrar pelos talhões da propriedade selecionada.

### `GET /predicoes/janelas` — já chamado pelo frontend (`api.predicoes.janelas`)
```sql
SELECT talhao_codigo, colheita_inicio, colheita_fim, exportacao_inicio, exportacao_fim,
       destino_exportacao AS destino, motivo, confianca_colheita_pct AS confianca
FROM analytics.predicoes_talhao
```
```json
[{ "talhao_codigo": "T-01", "colheita_inicio": "2026-12-08", "colheita_fim": "2026-12-19",
   "exportacao_inicio": "2026-12-15", "exportacao_fim": "2026-12-29", "destino": "Filadélfia (EUA)",
   "motivo": "31% das horas acima da faixa…", "confianca": 73 }]
```
⚠️ O mapper `janela.toUI` do frontend usa a data fixa `2026-11-23` como "hoje" — trocar por `new Date()`.

### `GET /predicoes/talhoes` — tabela "Previsão por talhão" e cards do topo
```sql
SELECT talhao_codigo, cultura, variedade, produtividade_prevista, produtividade_p05, produtividade_p95,
       variacao_safra_anterior_pct, colheita_inicio, colheita_fim, confianca_colheita_pct, gerado_em
FROM analytics.predicoes_talhao ORDER BY talhao_codigo
```

### `GET /predicoes/vigor/:talhaoCodigo` — gráfico "Comportamento do cultivo"
```json
{ "medido":   [{ "data": "2026-11-20", "vigor": 0.59, "observado": true }],
  "previsto": [{ "data": "2026-12-01", "vigor_previsto": 0.61, "vigor_p05": 0.55, "vigor_p95": 0.66 }] }
```
`medido` = `analytics.vigor_medido`; `previsto` = `analytics.predicoes_vigor_curva`. A faixa p05–p95 é o "Intervalo 90%".

### `GET /predicoes/fatores` — card "Fatores do modelo"
`SELECT fator, peso_pct FROM analytics.modelo_fatores ORDER BY peso_pct DESC LIMIT 5`

### `GET /predicoes/modelo` — faixa "Modelo treinado com N safras…"
```sql
SELECT max(n_safras_treino) AS n_safras, max(n_sensores) AS n_sensores, max(treinado_em) AS treinado_em
FROM analytics.modelo_metadados
```
Texto sugerido: *"Modelo treinado com {n_safras} safras e leituras de {n_sensores} sensores. Última atualização há {Δ}."*

### `GET /predicoes/risco` — card "Risco climático · 7 dias"
`SELECT * FROM analytics.risco_climatico_fazenda WHERE fazenda_id = $1` + lista de `analytics.risco_climatico_talhao` com `nivel <> 'Baixo'`.

## Pontos de alinhamento

1. **Códigos de sensor.** O backend em produção usa `ESP32-TALHAO-01` e `SEN-TH-02`; o cadastro do frontend usa
   `SN-101`…`SN-302`. Leituras com código desconhecido vão para `silver.leituras_quarentena` (motivo
   `sensor_nao_cadastrado`). Precisamos de um cadastro único de sensores (sugestão: o do backend passa a usar os códigos `SN-…`).
2. **IDs de fazenda.** `fazenda_id` nas tabelas `analytics` segue o cadastro do frontend (1 Santa Helena, 2 Boa Vista,
   3 Três Irmãos). Use `talhao_codigo` para juntar com o banco do backend.
3. **Leitura pública.** `GET /readings` está sem JWT hoje; o extrator funciona com ou sem chave, então pode proteger.
4. **Dados simulados.** Histórico de leituras, safras e vigor é simulado (ver README). A tela não deve mostrar valores fixos:
   tudo acima vem das tabelas.

---

# Contrato `/mercado` — tela Mercado

Atualizado em 27/11/2026. O modelo de preço roda no Databricks (tarefa `m03_mercado` do job diário). As tabelas
abaixo chegam ao Postgres no schema `analytics`, pelo mesmo caminho das predições.
**Não é preciso FastAPI**: a tela consulta referências e previsões que mudam uma vez por dia. Os novos endpoints
convivem com o módulo `mercado` que já existe (CRUD ComexStat/PTAX) e só leem `analytics.*`.

| Tabela | Grão | Colunas principais |
|---|---|---|
| `mercado_serie` | cultura × praça × dia (365 dias) | cultura, praca, data, preco_brl_kg, origem_preco |
| `mercado_referencias` | cultura × praça (hoje) | preco_atual, variacao_30d_pct, variacao_90d_pct, minima_90d, maxima_90d, frete_brl_kg, liquido_brl_kg, preco_previsto_30d, liquido_previsto_30d, melhor_liquido, melhor_liquido_previsto_30d, origem_preco |
| `mercado_previsao` | cultura × praça × horizonte (7, 14, 30 dias) | data_alvo, preco_atual, preco_previsto, preco_p05, preco_p95, variacao_prevista_pct |
| `mercado_modelo_metadados` | horizonte | mape, mape_ingenuo, mape_sazonal, mae_brl, acerto_direcao, cobertura_90, treinado_em |

Praças (valores exatos de `praca`): `Petrolina (PE)`, `CEAGESP (SP)`, `Roterdã (Países Baixos)`, `Filadélfia (EUA)`,
`Londres (Reino Unido)`. Culturas: `Manga`, `Uva`, `Goiaba`, `Melão`.

### `GET /mercado/serie?cultura=Manga&destino=Roterdã (Países Baixos)&periodo=90d` — já chamado pelo frontend
O mapper atual lê só `preco` de cada linha, em ordem de data.
```sql
SELECT data, preco_brl_kg AS preco FROM analytics.mercado_serie
WHERE cultura = $1 AND praca = $2 AND data > current_date - $3::int   -- 30d → 30, 90d → 90, 12m → 365
ORDER BY data
```
Para `12m`, sugestão: média semanal (`date_trunc('week', data)`) para não mandar 365 pontos.
Variação, mínima e máxima do card = calculadas pela API sobre a série retornada (primeiro × último ponto, min, max).

### `GET /mercado/referencias?cultura=Manga` — tabela "Referências por praça"
```sql
SELECT praca, preco_atual, variacao_30d_pct, frete_brl_kg, liquido_brl_kg, melhor_liquido,
       preco_previsto_30d, liquido_previsto_30d, melhor_liquido_previsto_30d, origem_preco
FROM analytics.mercado_referencias WHERE cultura = $1 ORDER BY praca
```
"Melhor líquido" = linha com `melhor_liquido = true`. A coluna "Variação" da tela = `variacao_30d_pct`.

### `GET /mercado/previsao?cultura=Manga&destino=Roterdã (Países Baixos)` — novo (previsão no gráfico ou num card)
```sql
SELECT horizonte_dias, data_alvo, preco_atual, preco_previsto, preco_p05, preco_p95, variacao_prevista_pct
FROM analytics.mercado_previsao WHERE cultura = $1 AND praca = $2 ORDER BY horizonte_dias
```
Sugestão de tela: prolongar a linha do gráfico com `preco_previsto` e uma faixa `preco_p05`–`preco_p95`,
como na curva de vigor das Predições de IA.

### Dados reais × simulados
`origem_preco` diz de onde vem cada série: `simulado` ou `simulado_ancorado_comexstat` (forma mensal do preço real
de exportação FOB/kg da ComexStat e câmbio PTAX real, baixados pelo GitHub Actions). Não há fonte pública diária de
preço por praça; o frete é parâmetro ilustrativo. Manter o aviso "valores ilustrativos" na tela.
