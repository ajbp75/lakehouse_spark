# 📦 Sonar – Teste Técnico Engenharia de Dados (Lakehouse)

## 📌 Visão Geral

Este projeto implementa um mini fluxo **Lakehouse (Bronze → Silver → Gold)** utilizando **PySpark (equivalente ao Microsoft Fabric)**.

O objetivo foi:

* Construir modelagem dimensional mínima (Star Schema)
* Garantir idempotência do pipeline
* Implementar Data Quality (DQ)
* Gerar métricas via SQL
* Produzir logs simples de execução

O pipeline pode ser executado múltiplas vezes sem gerar duplicações na camada Gold.

---

# 🏗 Arquitetura

```
data (raw CSVs)
   ↓
Bronze (ingestão)
   ↓
Silver (limpeza + deduplicação + tratamento)
   ↓
Gold (modelo dimensional final)
   ↓
DQ + Logs
   ↓
SQL Metrics
```

---

# 🥉 Bronze Layer

Responsável apenas por ingestão dos arquivos CSV:

* `customers.csv`
* `work_orders.csv`
* `parts_sales.csv`

Nenhuma transformação é aplicada nessa camada.

---

# 🥈 Silver Layer

Nesta camada foram aplicadas as regras de negócio.

## 🔹 Deduplicação

| Tabela      | Chave         | Critério                |
| ----------- | ------------- | ----------------------- |
| customers   | customer_id   | manter maior created_at |
| work_orders | work_order_id | manter maior updated_at |
| parts_sales | sale_id       | manter maior updated_at |

A deduplicação é determinística e garante consistência entre execuções.

---

## 🔹 Tratamento de Nulos

### fact_work_order

* `order_date` nulo → removido (necessário para análises temporais)
* `customer_id` nulo → substituído por `-1` (UNKNOWN)

### fact_parts_sales

* `unit_price` nulo → substituído por `0`
* `work_order_id` nulo → removido

---

## 🔹 Tratamento de Órfãos

### work_orders sem customer correspondente

Foi criado um registro especial na dimensão:

```
customer_id = -1
customer_name = "UNKNOWN"
```

Isso preserva o fato e mantém integridade referencial.

### parts_sales sem work_order correspondente

Registros removidos por inconsistência estrutural.

---

# 🥇 Gold Layer (Star Schema)

## 📌 dim_customer.csv

* customer_id
* customer_name
* segment
* state
* * registro UNKNOWN

## 📌 fact_work_order.csv

* work_order_id
* customer_id
* order_date
* status
* labor_hours
* labor_cost

## 📌 fact_parts_sales.csv

* sale_id
* work_order_id
* sku
* quantity
* unit_price
* total_price
* sale_date

---

# 📊 Data Quality (DQ)

Arquivo: `dq/dq_results.csv`

Checks implementados:

1. Taxa de nulos em colunas críticas
2. Taxa de duplicidade por chave
3. Taxa de órfãos (fato → dimensão)

Cada check contém:

* check_name
* table_name
* metric_value
* threshold
* status (PASS/FAIL)
* details

---

# 📋 Log de Execução

Arquivo: `dq/pipeline_runs.csv`

Campos:

* run_id
* started_at
* ended_at
* duration_seconds
* rows_dim_customer
* rows_fact_work_order
* rows_fact_parts_sales

---

# 🧮 SQL Metrics

Arquivo: `sql/metrics.sql`

Consultas incluídas:

1. Receita total de peças por cliente (últimos 90 dias)
2. Ordens por status por mês
3. Ticket médio de peças por ordem

---

# 🔁 Idempotência

O pipeline:

* Sempre lê os dados originais
* Deduplica deterministicamente
* Escreve os resultados finais em modo overwrite

Rodar múltiplas vezes não gera duplicações.

---

# ▶️ Como Executar

1. Criar ambiente virtual:

```
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2. Executar pipeline:

```
python pipeline.py
```

3. Os arquivos finais serão gerados dentro de:

```
submission_<SEU_NOME>/
```

---

# ⚠️ Limitações

* Não implementa carga incremental (apenas full refresh)
* Não há testes automatizados (priorização devido ao tempo do teste)
* Não há versionamento de schema

---

# 🚀 Próximos Passos (em ambiente real)

* Implementar carga incremental com merge
* Adicionar testes automatizados (pytest + validações de DQ)
* Orquestração (Airflow/Fabric Pipeline)
* Monitoramento estruturado
* Versionamento de schema

---

# 🧠 Observação Final

A solução foi construída priorizando:

* Clareza arquitetural
* Integridade referencial
* Consistência de regras
* Reprodutibilidade
* Governança básica de dados
