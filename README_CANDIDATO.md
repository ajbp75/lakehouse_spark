# Teste Técnico — Engenharia de Dados (Microsoft Fabric) — Sonar

**Tempo esperado:** 1 a 2 horas  
**Entrega:** um arquivo `.zip` com os artefatos finais (tabelas Gold + checks de qualidade + SQL + README).

## 1) Contexto
Você recebeu 3 arquivos CSV na pasta `data/`:
- `customers.csv`
- `work_orders.csv`
- `parts_sales.csv`

Eles simulam dados reais (clientes, ordens de serviço e venda de peças) e **contêm problemas propositais** (duplicidades, nulos e chaves quebradas).

## 2) Objetivo
Construir um mini **Lakehouse** no **Microsoft Fabric** (ou equivalente) com camadas **Bronze → Silver → Gold**, com:
- modelagem dimensional mínima (Star Schema),
- métricas via SQL,
- checks de qualidade (DQ),
- log mínimo de execução.

## 3) Regras
- Você pode usar Notebook (PySpark) e/ou SQL e/ou Dataflow Gen2.
- Pode usar IA como apoio, mas **o que vale é consistência e entendimento** — faremos uma defesa rápida depois.
- Seu resultado precisa ser **idempotente** (rodar 2x não pode duplicar dados na Gold).

## 4) Tarefas (o que precisa existir na entrega)
### 4.1 Gold (final)
Gere as tabelas abaixo e exporte para CSV dentro de `gold/`:

#### `gold/dim_customer.csv`
Colunas mínimas:
- `customer_id` (chave)
- `customer_name`
- `segment`
- `state`

Regras:
- Deduplicar por `customer_id`. Se houver conflito, documente seu critério (ex.: manter o registro mais recente por `created_at`).

#### `gold/fact_work_order.csv`
Colunas mínimas:
- `work_order_id` (chave)
- `customer_id` (FK)
- `order_date`
- `status`
- `labor_hours`
- `labor_cost`

Regras:
- Deduplicar por `work_order_id` (use `updated_at` como critério sugerido).
- Tratar `customer_id` nulo e `order_date` nulo (decida e justifique).
- Tratar **órfãos** (work_orders cujo customer_id não existe na dim): você decide (ex.: rejeitar, criar 'UNKNOWN', ou separar em rejects) e justifica.

#### `gold/fact_parts_sales.csv`
Colunas mínimas:
- `sale_id` (chave)
- `work_order_id` (FK)
- `sku`
- `quantity`
- `unit_price`
- `total_price`
- `sale_date`

Regras:
- Deduplicar por `sale_id` (use `updated_at` sugerido).
- Tratar `work_order_id` nulo, `unit_price` nulo e órfãos (sale sem work_order).

> **Opcional (bônus):** `gold/dim_date.csv` com calendário e chaves para facilitar BI.

### 4.2 Data Quality (DQ)
Crie `dq/dq_results.csv` com pelo menos 3 checks e seus resultados.
Formato sugerido (uma linha por check):
- `check_name`
- `table_name`
- `metric_value`
- `threshold`
- `status` (PASS/FAIL)
- `details` (texto curto)

Checks mínimos:
1) taxa de nulos em colunas críticas (por tabela)
2) taxa de duplicidade por chave
3) taxa de órfãos (fato → dimensão)

Crie também `dq/pipeline_runs.csv` com:
- `run_id`
- `started_at`
- `ended_at`
- `duration_seconds`
- `rows_dim_customer`
- `rows_fact_work_order`
- `rows_fact_parts_sales`

### 4.3 SQL (métricas)
Coloque em `sql/metrics.sql` **3 queries**:
1) Receita total de peças por cliente (últimos 90 dias)
2) Ordens por status por mês
3) Ticket médio de peças por ordem (média de total_price agrupado por work_order)

## 5) Entrega
Envie um `.zip` com esta estrutura:

```
submission_<SEU_NOME>/
  README.md
  metadata.json
  gold/
    dim_customer.csv
    fact_work_order.csv
    fact_parts_sales.csv
    dim_date.csv (opcional)
  dq/
    dq_results.csv
    pipeline_runs.csv
  sql/
    metrics.sql
  notebook/
    notebook.ipynb (opcional)
```

### 5.1 `metadata.json` (obrigatório)
Inclua:
- seu nome
- rota usada (notebook/sql/dataflow)
- regra de deduplicação
- como tratou nulos e órfãos
- thresholds usados para DQ
- `dataset_id` (copie do arquivo `DATASET_ID.txt`)

Exemplo:
```json
{
  "candidate": "Fulano",
  "route": "A_notebook",
  "dedup_rule": "work_order_id keep max(updated_at)",
  "null_handling": {
    "fact_work_order.customer_id": "drop",
    "fact_work_order.order_date": "drop"
  },
  "orphan_handling": "rejects",
  "dq_thresholds": {
    "null_rate_critical_max": 0.005,
    "dup_rate_key_max": 0.001,
    "orphan_rate_max": 0.002
  },
  "dataset_id": "SONAR_FABRIC_TEST_V1_2026-02-06"
}
```

### 5.2 README do candidato (obrigatório, 1 página)
Explique:
- o que você fez (Bronze/Silver/Gold)
- decisões (dedup, nulos, órfãos)
- como reproduzir
- limitações e próximos passos

Boa sorte!
