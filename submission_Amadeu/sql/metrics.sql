-- 1) Receita total de peças por cliente (últimos 90 dias)
SELECT
    c.customer_id,
    c.customer_name,
    SUM(ps.total_price) AS total_revenue
FROM fact_parts_sales ps
JOIN fact_work_order wo
    ON ps.work_order_id = wo.work_order_id
JOIN dim_customer c
    ON wo.customer_id = c.customer_id
WHERE ps.sale_date >= CURRENT_DATE - INTERVAL '90' DAY
GROUP BY c.customer_id, c.customer_name;


-- 2) Ordens por status por mês
SELECT
    DATE_TRUNC('month', order_date) AS month,
    status,
    COUNT(*) AS total_orders
FROM fact_work_order
GROUP BY DATE_TRUNC('month', order_date), status
ORDER BY month, status;


-- 3) Ticket médio de peças por ordem
SELECT
    work_order_id,
    AVG(total_price) AS avg_ticket
FROM fact_parts_sales
GROUP BY work_order_id;
