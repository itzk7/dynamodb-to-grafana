"""
Gold Layer: Delta refresh using Iceberg tables
Preserves historical data, only refreshes the last N days
"""
import json
import os
import boto3
import time

athena_client = boto3.client('athena')
glue_client = boto3.client('glue')

SILVER_DB = os.environ['SILVER_DATABASE']
GOLD_DB = os.environ['GOLD_DATABASE']
ATHENA_WORKGROUP = os.environ['ATHENA_WORKGROUP']
ATHENA_OUTPUT = os.environ['ATHENA_OUTPUT_LOCATION']
S3_BUCKET = os.environ['S3_BUCKET']

# Number of days to refresh on each run
REFRESH_DAYS = int(os.environ.get('REFRESH_DAYS', '3'))


def execute_athena_query(query, database=None):
    """Execute Athena query and wait for completion"""
    db = database if database else GOLD_DB

    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': db},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT},
        WorkGroup=ATHENA_WORKGROUP
    )

    query_id = response['QueryExecutionId']

    for _ in range(120):
        status = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']

        if state == 'SUCCEEDED':
            return query_id
        elif state in ['FAILED', 'CANCELLED']:
            reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
            raise Exception(f"Query {state}: {reason}")
        time.sleep(3)

    raise Exception("Query timeout")


def table_exists(table_name):
    """Check if table exists in Glue Catalog"""
    try:
        glue_client.get_table(DatabaseName=GOLD_DB, Name=table_name)
        return True
    except glue_client.exceptions.EntityNotFoundException:
        return False
    except:
        return False


def ensure_daily_sales_table():
    """Create daily_sales_by_region Iceberg table if it doesn't exist"""
    if table_exists('daily_sales_by_region'):
        return

    print("  Creating Iceberg table daily_sales_by_region...")
    execute_athena_query(f"""
        CREATE TABLE daily_sales_by_region (
            order_date DATE,
            region STRING,
            order_count BIGINT,
            total_revenue DOUBLE,
            avg_order_value DOUBLE,
            unique_customers BIGINT,
            computed_at TIMESTAMP
        )
        LOCATION 's3://{S3_BUCKET}/gold/daily_sales_by_region/'
        TBLPROPERTIES ('table_type'='ICEBERG')
    """)
    print("Table created")


def ensure_product_performance_table():
    """Create product_performance Iceberg table if it doesn't exist"""
    if table_exists('product_performance'):
        return

    print("  Creating Iceberg table product_performance...")
    execute_athena_query(f"""
        CREATE TABLE product_performance (
            productid STRING,
            product_name STRING,
            current_price DOUBLE,
            current_stock INT,
            stock_status STRING,
            computed_at TIMESTAMP
        )
        LOCATION 's3://{S3_BUCKET}/gold/product_performance/'
        TBLPROPERTIES ('table_type'='ICEBERG')
    """)
    print("Table created")


def ensure_key_metrics_table():
    """Create key_metrics Iceberg table if it doesn't exist"""
    if table_exists('key_metrics'):
        return

    print("  Creating Iceberg table key_metrics...")
    execute_athena_query(f"""
        CREATE TABLE key_metrics (
            metric_name STRING,
            metric_value DOUBLE,
            computed_at TIMESTAMP
        )
        LOCATION 's3://{S3_BUCKET}/gold/key_metrics/'
        TBLPROPERTIES ('table_type'='ICEBERG')
    """)
    print("Table created")


def refresh_daily_sales_by_region():
    """
    Daily sales by region — delta refresh.
    Deletes last N days of data, then re-inserts fresh aggregates.
    Historical data beyond N days is preserved.
    """
    print(f"Daily Sales by Region (delta refresh: last {REFRESH_DAYS} days)")

    ensure_daily_sales_table()

    # Step 1: Delete stale data for the refresh window
    print(f"  Deleting data for last {REFRESH_DAYS} days...")
    execute_athena_query(f"""
        DELETE FROM daily_sales_by_region
        WHERE order_date >= CURRENT_DATE - INTERVAL '{REFRESH_DAYS}' DAY
    """)

    # Step 2: Insert fresh aggregates for the refresh window
    print(f"  Inserting fresh aggregates...")
    execute_athena_query(f"""
        INSERT INTO daily_sales_by_region
        SELECT
            CAST(date_parse(orderdate, '%Y-%m-%d %H:%i:%s') AS DATE) as order_date,
            customer_region as region,
            COUNT(*) as order_count,
            SUM(totalamount) as total_revenue,
            AVG(totalamount) as avg_order_value,
            COUNT(DISTINCT customerid) as unique_customers,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) as computed_at
        FROM "{SILVER_DB}".orders_enriched
        WHERE CAST(date_parse(orderdate, '%Y-%m-%d %H:%i:%s') AS DATE)
              >= CURRENT_DATE - INTERVAL '{REFRESH_DAYS}' DAY
            AND customer_region IS NOT NULL
        GROUP BY CAST(date_parse(orderdate, '%Y-%m-%d %H:%i:%s') AS DATE),
                 customer_region
    """)
    print(f"Delta refresh complete (last {REFRESH_DAYS} days refreshed, history preserved)")


def refresh_product_performance():
    """
    Product performance — full replace via Iceberg.
    Deletes all rows, inserts current snapshot.
    Products are a small dataset so full replace is fine.
    """
    print("Product Performance (full replace)")

    ensure_product_performance_table()

    # Step 1: Delete all existing rows
    print("Clearing existing data...")
    execute_athena_query("DELETE FROM product_performance WHERE 1=1")

    # Step 2: Insert current product snapshot
    print("  Inserting current product data...")
    execute_athena_query(f"""
        INSERT INTO product_performance
        SELECT
            p.productid,
            p.name as product_name,
            p.price as current_price,
            p.stocklevel as current_stock,
            CASE
                WHEN p.stocklevel < 10 THEN 'Critical'
                WHEN p.stocklevel < 50 THEN 'Low'
                ELSE 'Good'
            END as stock_status,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) as computed_at
        FROM "{SILVER_DB}".products_enriched p
    """)
    print("Full replace complete")


def refresh_key_metrics():
    """
    Key metrics — full replace via Iceberg.
    Deletes all rows, inserts current KPI snapshot.
    """
    print("Key Metrics (full replace)")

    ensure_key_metrics_table()

    # Step 1: Delete all existing rows
    print("  Clearing existing data...")
    execute_athena_query("DELETE FROM key_metrics WHERE 1=1")

    # Step 2: Insert current KPIs
    print("  Computing current metrics...")
    execute_athena_query(f"""
        INSERT INTO key_metrics

        SELECT 'orders_today' as metric_name,
               CAST(COUNT(*) AS DOUBLE) as metric_value,
               CAST(CURRENT_TIMESTAMP AS TIMESTAMP) as computed_at
        FROM "{SILVER_DB}".orders_enriched
        WHERE CAST(date_parse(orderdate, '%Y-%m-%d %H:%i:%s') AS DATE) = CURRENT_DATE

        UNION ALL

        SELECT 'revenue_today',
               CAST(COALESCE(SUM(totalamount), 0) AS DOUBLE),
               CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
        FROM "{SILVER_DB}".orders_enriched
        WHERE CAST(date_parse(orderdate, '%Y-%m-%d %H:%i:%s') AS DATE) = CURRENT_DATE

        UNION ALL

        SELECT 'revenue_7d',
               CAST(COALESCE(SUM(totalamount), 0) AS DOUBLE),
               CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
        FROM "{SILVER_DB}".orders_enriched
        WHERE CAST(date_parse(orderdate, '%Y-%m-%d %H:%i:%s') AS DATE) >= CURRENT_DATE - INTERVAL '7' DAY

        UNION ALL

        SELECT 'revenue_30d',
               CAST(COALESCE(SUM(totalamount), 0) AS DOUBLE),
               CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
        FROM "{SILVER_DB}".orders_enriched
        WHERE CAST(date_parse(orderdate, '%Y-%m-%d %H:%i:%s') AS DATE) >= CURRENT_DATE - INTERVAL '30' DAY

        UNION ALL

        SELECT 'revenue_90d',
               CAST(COALESCE(SUM(totalamount), 0) AS DOUBLE),
               CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
        FROM "{SILVER_DB}".orders_enriched
        WHERE CAST(date_parse(orderdate, '%Y-%m-%d %H:%i:%s') AS DATE) >= CURRENT_DATE - INTERVAL '90' DAY
    """)
    print("Current metrics computed")


def lambda_handler(event, context):
    """Gold layer delta refresh with historical preservation"""
    print("GOLD LAYER REFRESH (Iceberg Delta)")
    print(f"   Refresh window: last {REFRESH_DAYS} days")

    try:
        refresh_daily_sales_by_region()
        refresh_product_performance()
        refresh_key_metrics()
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Gold layer refreshed',
                'refresh_days': REFRESH_DAYS,
                'strategy': 'iceberg_delta'
            })
        }

    except Exception as e:
        print(f"ERROR: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
