import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType
from pyspark.sql.window import Window
import os
import shutil
from pathlib import Path

#Sessão do Spark
def create_spark():
    return (
        SparkSession.builder
        .appName("Sonar Lakehouse Test")
        .master("local[*]")
        .getOrCreate()
    )

#Utils
def write_single_csv(df, output_path):
    temp_path = output_path + "_tmp"

    # escreve em pasta temporária
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_path)

    # encontra arquivo part-*.csv
    temp_dir = Path(temp_path)
    csv_file = next(temp_dir.glob("part-*.csv"))

    # move e renomeia
    final_path = Path(output_path)
    shutil.move(str(csv_file), str(final_path))

    # remove pasta temporária
    shutil.rmtree(temp_path)

#bronze layer
def bronze(spark):
    customers = spark.read.option("header", True).csv("../data/customers.csv")
    work_orders = spark.read.option("header", True).csv("../data/work_orders.csv")
    parts_sales = spark.read.option("header", True).csv("../data/parts_sales.csv")

    return customers, work_orders, parts_sales

#silver layer
def silver(customers, work_orders, parts_sales):

    # Dedup customers
    w = Window.partitionBy("customer_id").orderBy(F.col("created_at").desc())
    customers_silver = (
        customers
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
    )

    # Dedup work_orders
    w = Window.partitionBy("work_order_id").orderBy(F.col("updated_at").desc())
    work_orders_silver = (
        work_orders
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
        .filter(F.col("order_date").isNotNull())
    )

    # Dedup parts_sales
    w = Window.partitionBy("sale_id").orderBy(F.col("updated_at").desc())

    parts_sales_silver = (
        parts_sales
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")

        # Tipagem explícita
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn(
            "unit_price",
            F.coalesce(F.col("unit_price"), F.lit(0))
            .cast(DecimalType(10, 2))
        )

        # Cálculo monetário seguro
        .withColumn(
            "total_price",
            (F.col("quantity") * F.col("unit_price"))
            .cast(DecimalType(12, 2))
        )
    )

    return customers_silver, work_orders_silver, parts_sales_silver
def gold(customers_silver, work_orders_silver, parts_sales_silver):

    spark = customers_silver.sparkSession

    unknown = spark.createDataFrame(
        [(-1, "UNKNOWN", "UNKNOWN", "UNKNOWN")],
        ["customer_id", "customer_name", "segment", "state"]
    )

    dim_customer = customers_silver.select(
        "customer_id", "customer_name", "segment", "state"
    ).unionByName(unknown)

    fact_work_order = work_orders_silver.join(
        dim_customer.select("customer_id"),
        on="customer_id",
        how="left"
    ).withColumn(
        "customer_id",
        F.when(F.col("customer_id").isNull(), F.lit(-1))
        .otherwise(F.col("customer_id"))
    ).select(
        "work_order_id",
        "customer_id",
        "order_date",
        "status",
        "labor_hours",
        "labor_cost"
    )

    fact_parts_sales = parts_sales_silver.join(
        fact_work_order.select("work_order_id"),
        on="work_order_id",
        how="inner"
    ).select(
        "sale_id",
        "work_order_id",
        "sku",
        "quantity",
        "unit_price",
        "total_price",
        "sale_date"
    )

    return dim_customer, fact_work_order, fact_parts_sales

    from pyspark.sql.types import IntegerType

def build_dim_date(fact_work_order, fact_parts_sales):
    spark = fact_work_order.sparkSession

    # Coletar datas distintas dos fatos
    dates_work_order = fact_work_order.select(
        F.col("order_date").alias("date")
    )

    dates_parts_sales = fact_parts_sales.select(
        F.col("sale_date").alias("date")
    )

    dates_union = (
        dates_work_order
        .union(dates_parts_sales)
        .filter(F.col("date").isNotNull())
        .distinct()
    )

    dim_date = (
        dates_union
        .withColumn("date_id", F.date_format("date", "yyyyMMdd").cast(IntegerType()))
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("day_of_week", F.date_format("date", "E"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek("date").isin([1, 7]), F.lit(True))
             .otherwise(F.lit(False))
        )
        .select(
            "date_id",
            "date",
            "year",
            "month",
            "month_name",
            "day",
            "day_of_week",
            "is_weekend"
        )
        .orderBy("date")
    )

    return dim_date

def dq(dim_customer, fact_work_order, fact_parts_sales):

    results = []

    # ==========================
    # NULL RATE CHECK
    # ==========================

    total_dim = dim_customer.count()
    null_customer_id = dim_customer.filter(F.col("customer_id").isNull()).count()
    null_rate = null_customer_id / total_dim if total_dim > 0 else 0

    results.append((
        "null_rate_customer_id",
        "dim_customer",
        null_rate,
        0.01,
        "PASS" if null_rate <= 0.01 else "FAIL",
        "customer_id should not be null"
    ))

    # ==========================
    # DUPLICATE RATE CHECK
    # ==========================

    total_work = fact_work_order.count()
    distinct_work = fact_work_order.select("work_order_id").distinct().count()
    dup_rate = (total_work - distinct_work) / total_work if total_work > 0 else 0

    results.append((
        "duplicate_rate_work_order",
        "fact_work_order",
        dup_rate,
        0.0,
        "PASS" if dup_rate == 0 else "FAIL",
        "work_order_id must be unique"
    ))

    # ==========================
    # ORPHAN CHECK (parts_sales → work_order)
    # ==========================

    orphan_count = fact_parts_sales.join(
        fact_work_order.select("work_order_id"),
        on="work_order_id",
        how="left_anti"
    ).count()

    orphan_rate = orphan_count / fact_parts_sales.count() if fact_parts_sales.count() > 0 else 0

    results.append((
        "orphan_rate_parts_sales",
        "fact_parts_sales",
        orphan_rate,
        0.0,
        "PASS" if orphan_rate == 0 else "FAIL",
        "sales must reference valid work_order"
    ))

    dq_df = dim_customer.sparkSession.createDataFrame(
        results,
        ["check_name", "table_name", "metric_value", "threshold", "status", "details"]
    )

    return dq_df

def main():
    spark = create_spark()

    run_id = str(uuid.uuid4())
    started_at = datetime.utcnow()

    customers, work_orders, parts_sales = bronze(spark)

    customers_silver, work_orders_silver, parts_sales_silver = silver(
        customers, work_orders, parts_sales
    )

    dim_customer, fact_work_order, fact_parts_sales = gold(
        customers_silver, work_orders_silver, parts_sales_silver
    )

    dim_date = build_dim_date(fact_work_order, fact_parts_sales)

    
    # =========================================
    # Validação local de métricas (Spark SQL)
    #=========================================
    #Caso eu queira testar as métricas, remover comentários
    '''
    dim_customer.createOrReplaceTempView("dim_customer")
    fact_work_order.createOrReplaceTempView("fact_work_order")
    fact_parts_sales.createOrReplaceTempView("fact_parts_sales")

    #Receita por 90 dias
    spark.sql("""
            SELECT
                c.customer_id,
                c.customer_name,
                SUM(ps.total_price) AS total_revenue
            FROM fact_parts_sales ps
            JOIN fact_work_order wo
                ON ps.work_order_id = wo.work_order_id
            JOIN dim_customer c
                ON wo.customer_id = c.customer_id
            WHERE ps.sale_date >= date_sub(current_date(), 90)
            GROUP BY c.customer_id, c.customer_name
            ORDER BY total_revenue DESC
            """).show(10, truncate=False)
    
    #Ordens por status/mês
    spark.sql("""
            SELECT
                date_trunc('month', order_date) AS month,
                status,
                COUNT(*) AS total_orders
            FROM fact_work_order
            GROUP BY date_trunc('month', order_date), status
            ORDER BY month, status
            """).show(10, truncate=False)
    
    #Ticket médio de peças por ordem
    spark.sql("""
            SELECT
                work_order_id,
                AVG(total_price) AS avg_ticket
            FROM fact_parts_sales
            GROUP BY work_order_id
            ORDER BY avg_ticket DESC
            """).show(10, truncate=False)
    '''

    # Write Gold
    write_single_csv(dim_customer, "../submission_Amadeu/gold/dim_customer.csv")
    write_single_csv(fact_work_order, "../submission_Amadeu/gold/fact_work_order.csv")
    write_single_csv(fact_parts_sales, "../submission_Amadeu/gold/fact_parts_sales.csv")
    write_single_csv(dim_date, "../submission_Amadeu/gold/dim_date.csv")

    #
    # DQ
    dq_df = dq(dim_customer, fact_work_order, fact_parts_sales)
    write_single_csv(dq_df, "../submission_Amadeu/dq/dq_results.csv")

    # Metrics for pipeline run
    rows_dim_customer = dim_customer.count()
    rows_fact_work_order = fact_work_order.count()
    rows_fact_parts_sales = fact_parts_sales.count()

    ended_at = datetime.utcnow()
    duration_seconds = (ended_at - started_at).total_seconds()

    pipeline_run = spark.createDataFrame(
        [
            (
                run_id,
                started_at.isoformat(),
                ended_at.isoformat(),
                duration_seconds,
                rows_dim_customer,
                rows_fact_work_order,
                rows_fact_parts_sales
            )
        ],
        [
            "run_id",
            "started_at",
            "ended_at",
            "duration_seconds",
            "rows_dim_customer",
            "rows_fact_work_order",
            "rows_fact_parts_sales"
        ]
    )

    write_single_csv(
        pipeline_run,
        "../submission_Amadeu/dq/pipeline_runs.csv"
    )

    print("Pipeline executed successfully.")



if __name__ == "__main__":

    main()
