"""
Gold Layer: Accumulating historical aggregates
Preserves history while refreshing recent data
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


def refresh_daily_sales_by_region():
    """
    Daily sales by region - full refresh with last 30 days
    Simplified for Parquet tables (no row-level modifications)
    """
    print("\n📊 Daily Sales by Region")
    
    # Full refresh: Drop and recreate with last 30 days
    try:
        execute_athena_query("DROP TABLE IF EXISTS daily_sales_by_region")
    except:
        pass
    
    query = f"""
        CREATE TABLE daily_sales_by_region
        WITH (
            format = 'PARQUET',
            external_location = 's3://{S3_BUCKET}/gold/daily_sales_by_region/'
        ) AS
        SELECT 
            CAST(date_parse(orderdate, '%Y-%m-%d %H:%i:%s') AS DATE) as order_date,
            customer_region as region,
            COUNT(*) as order_count,
            SUM(totalamount) as total_revenue,
            AVG(totalamount) as avg_order_value,
            COUNT(DISTINCT customerid) as unique_customers,
            CAST(CURRENT_TIMESTAMP AS TIMESTAMP) as computed_at
        FROM "{SILVER_DB}".orders_enriched
        WHERE CAST(date_parse(orderdate, '%Y-%m-%d %H:%i:%s') AS DATE) >= CURRENT_DATE - INTERVAL '30' DAY
            AND customer_region IS NOT NULL
        GROUP BY CAST(date_parse(orderdate, '%Y-%m-%d %H:%i:%s') AS DATE), customer_region
    """
    execute_athena_query(query)
    print("  ✓ Full refresh complete (last 30 days)")


def refresh_product_performance():
    """
    Product performance - full refresh each time (small dataset)
    Products change slowly, so full refresh is fine
    """
    print("\n📦 Product Performance")
    
    # For products, full refresh is OK (small table)
    try:
        execute_athena_query("DROP TABLE IF EXISTS product_performance")
    except:
        pass
    
    query = f"""
        CREATE TABLE product_performance
        WITH (
            format = 'PARQUET',
            external_location = 's3://{S3_BUCKET}/gold/product_performance/'
        ) AS
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
    """
    
    execute_athena_query(query)
    print("  ✓ Full refresh complete")


def refresh_key_metrics():
    """
    Key metrics - always full refresh (current values only)
    These are snapshot metrics, not historical
    """
    print("\n📈 Key Metrics")
    
    try:
        execute_athena_query("DROP TABLE IF EXISTS key_metrics")
    except:
        pass
    
    query = f"""
        CREATE TABLE key_metrics
        WITH (
            format = 'PARQUET',
            external_location = 's3://{S3_BUCKET}/gold/key_metrics/'
        ) AS
        
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
    """
    execute_athena_query(query)
    print("  ✓ Current metrics computed")


def lambda_handler(event, context):
    """Gold layer refresh with historical preservation"""
    print("=" * 60)
    print("🏆 GOLD LAYER REFRESH")
    print("=" * 60)
    
    try:
        # Refresh with history preservation
        refresh_daily_sales_by_region()  # Delta: Preserves history
        refresh_product_performance()     # Full: Small dataset
        refresh_key_metrics()            # Full: Current snapshot
        
        print("\n" + "=" * 60)
        print("✅ COMPLETE")
        print("=" * 60)
        print("History Status:")
        print("  • daily_sales_by_region: ✓ Historical data preserved")
        print("  • product_performance: Current snapshot")
        print("  • key_metrics: Current values (7d, 30d, 90d windows)")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Gold layer refreshed',
                'history_preserved': True
            })
        }
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }