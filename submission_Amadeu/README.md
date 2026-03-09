# Sonar – Teste Técnico de Engenharia de Dados

## Visão Geral
Este projeto implementa um mini Lakehouse com as camadas Bronze, Silver e Gold
a partir de arquivos CSV simulando dados operacionais com problemas propositais
(duplicidades, nulos e chaves órfãs).

A solução foi desenvolvida em PySpark, com foco em idempotência, clareza de regras
de qualidade e modelagem dimensional mínima (Star Schema).

---

## Arquitetura

> Observação: a solução foi implementada em PySpark local como equivalente ao Microsoft Fabric, mantendo os mesmos conceitos de Lakehouse, camadas Bronze/Silver/Gold e modelagem analítica solicitados no teste.

- **Bronze**: ingestão dos CSVs originais sem alterações
- **Silver**: limpeza, tipagem, deduplicação e tratamento de nulos
- **Gold**: modelagem dimensional (1 dimensão + 2 fatos) pronta para consumo analítico
- **DQ**: checks de qualidade e log de execução da pipeline

Fluxo:
Bronze → Silver → Gold → DQ / Analytics

---

> Como melhoria opcional (bônus), foi incluída uma dimensão de datas (`dim_date`) derivada das datas dos fatos para facilitar análises temporais em ferramentas de BI.

## Principais Decisões Técnicas

### Deduplicação
- `customers`: deduplicado por `customer_id`, mantendo o registro mais recente via `created_at`
- `work_orders`: deduplicado por `work_order_id`, mantendo o registro mais recente via `updated_at`
- `parts_sales`: deduplicado por `sale_id`, mantendo o registro mais recente via `updated_at`

Foi utilizado `row_number()` com `Window.partitionBy().orderBy(desc)`.

### Tratamento de Nulos
- `fact_work_order.order_date`: registros descartados
- `fact_parts_sales.unit_price`: substituído por 0
- Recalculo seguro de `total_price` após tipagem numérica

### Tratamento de Órfãos
- `fact_work_order.customer_id` inexistente → atribuído para cliente `UNKNOWN (-1)`
- `fact_parts_sales` sem `work_order_id` válido → removido via inner join

---

## Data Quality (DQ)
Foram implementados checks mínimos:
- Taxa de nulos em colunas críticas
- Taxa de duplicidade por chave
- Taxa de órfãos entre fatos e dimensões

Os resultados estão em `dq/dq_results.csv` e o log de execução em `dq/pipeline_runs.csv`.

---

## SQL / Métricas
As métricas analíticas foram implementadas em SQL e estão disponíveis em:
`sql/metrics.sql`

Incluem:
1. Receita total de peças por cliente (últimos 90 dias)
2. Ordens por status por mês
3. Ticket médio de peças por ordem

---

## Como Executar

A partir da pasta `src`, execute:

```bash
python pipeline.py
