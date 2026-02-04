"""
Silver Layer: Cleanse and enrich data with MERGE deduplication
Key concept: Enrich orders with customer data (analytics value!)
Optimized for Athena Engine v3 with Iceberg
"""
import json
import os
import boto3
from datetime import datetime
import time
import io
import pyarrow as pa
import pyarrow.parquet as pq
from urllib.parse import unquote_plus

s3_client = boto3.client('s3')
athena_client = boto3.client('athena')
dynamodb = boto3.resource('dynamodb')

S3_BUCKET = os.environ['S3_BUCKET']
CUSTOMERS_TABLE = os.environ['CUSTOMERS_TABLE']
ATHENA_DATABASE = os.environ['ATHENA_DATABASE']
ATHENA_WORKGROUP = os.environ['ATHENA_WORKGROUP']
ATHENA_OUTPUT = os.environ['ATHENA_OUTPUT_LOCATION']


def execute_athena_query(query):
    """Execute Athena query and wait for completion"""
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT},
        WorkGroup=ATHENA_WORKGROUP
    )
    
    query_id = response['QueryExecutionId']
    
    # Wait for completion
    for _ in range(60):
        status = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']
        if state == 'SUCCEEDED':
            return query_id
        elif state in ['FAILED', 'CANCELLED']:
            reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
            raise Exception(f"Query {state}: {reason}")
        time.sleep(2)
    
    raise Exception("Query timeout")


def enrich_order_data(order):
    """
    Enrich orders with customer data (KEY ANALYTICS FEATURE!)
    This enables geographic analysis in dashboards
    """
    customer_id = order.get('CustomerID')
    if not customer_id:
        return order
    
    # Lookup customer from DynamoDB
    table = dynamodb.Table(CUSTOMERS_TABLE)
    response = table.get_item(Key={'CustomerID': customer_id})
    
    if 'Item' in response:
        customer = response['Item']
        order['customer_name'] = customer.get('Name', '')
        order['customer_region'] = customer.get('Region', '')
    
    return order


def transform_records(bronze_records):
    """Transform bronze to silver format with enrichment"""
    silver_records = {'orders': [], 'products': [], 'customers': []}
    
    for record in bronze_records:
        table = record['_table_name']
        event_time = record['_event_timestamp']
        
        if 'orders' in table.lower():
            enriched = enrich_order_data(record)
            silver_records['orders'].append({
                'orderid': enriched.get('OrderID'),
                'customerid': enriched.get('CustomerID'),
                'orderdate': enriched.get('OrderDate'),
                'totalamount': float(enriched.get('TotalAmount', 0)),
                'customer_name': enriched.get('customer_name', ''),
                'customer_region': enriched.get('customer_region', ''),
                'event_timestamp': event_time,
                'processing_timestamp': datetime.utcnow().isoformat()
            })
        
        elif 'products' in table.lower():
            silver_records['products'].append({
                'productid': record.get('ProductID'),
                'name': record.get('Name'),
                'price': float(record.get('Price', 0)),
                'stocklevel': int(record.get('StockLevel', 0)),
                'event_timestamp': event_time,
                'processing_timestamp': datetime.utcnow().isoformat()
            })
        
        elif 'customers' in table.lower():
            silver_records['customers'].append({
                'customerid': record.get('CustomerID'),
                'name': record.get('Name'),
                'region': record.get('Region'),
                'event_timestamp': event_time,
                'processing_timestamp': datetime.utcnow().isoformat()
            })
    
    return silver_records


def merge_to_iceberg(table_name, staging_path):
    """
    MERGE INTO Iceberg table using Athena Engine v3
    Uses INSERT INTO with SELECT for maximum compatibility
    """
    temp_table = f"temp_{table_name}_{int(datetime.utcnow().timestamp())}"
    
    # Schema definitions for temp table
    schema_defs = {
        'orders': """
            orderid STRING,
            customerid STRING,
            orderdate STRING,
            totalamount DOUBLE,
            customer_name STRING,
            customer_region STRING,
            event_timestamp STRING,
            processing_timestamp STRING
        """,
        'products': """
            productid STRING,
            name STRING,
            price DOUBLE,
            stocklevel INT,
            event_timestamp STRING,
            processing_timestamp STRING
        """,
        'customers': """
            customerid STRING,
            name STRING,
            region STRING,
            event_timestamp STRING,
            processing_timestamp STRING
        """
    }
    
    try:
        # Create external temp table from staging parquet
        staging_dir = staging_path.rsplit('/', 1)[0]
        create_external = f"""
            CREATE EXTERNAL TABLE {temp_table} (
                {schema_defs[table_name]}
            )
            STORED AS PARQUET
            LOCATION '{staging_dir}/'
        """
        execute_athena_query(create_external)
        
        # Create Iceberg table if not exists
        iceberg_tables = {
            'orders': f"""
                CREATE TABLE IF NOT EXISTS orders_enriched (
                    orderid STRING,
                    customerid STRING,
                    orderdate STRING,
                    totalamount DOUBLE,
                    customer_name STRING,
                    customer_region STRING,
                    event_timestamp STRING,
                    processing_timestamp STRING
                )
                LOCATION 's3://{S3_BUCKET}/silver/iceberg/orders/'
                TBLPROPERTIES ('table_type'='ICEBERG')
            """,
            'products': f"""
                CREATE TABLE IF NOT EXISTS products_enriched (
                    productid STRING,
                    name STRING,
                    price DOUBLE,
                    stocklevel INT,
                    event_timestamp STRING,
                    processing_timestamp STRING
                )
                LOCATION 's3://{S3_BUCKET}/silver/iceberg/products/'
                TBLPROPERTIES ('table_type'='ICEBERG')
            """,
            'customers': f"""
                CREATE TABLE IF NOT EXISTS customers_enriched (
                    customerid STRING,
                    name STRING,
                    region STRING,
                    event_timestamp STRING,
                    processing_timestamp STRING
                )
                LOCATION 's3://{S3_BUCKET}/silver/iceberg/customers/'
                TBLPROPERTIES ('table_type'='ICEBERG')
            """
        }
        execute_athena_query(iceberg_tables[table_name])
        
        # MERGE queries for Iceberg
        merge_queries = {
            'orders': f"""
                MERGE INTO orders_enriched t
                USING {temp_table} s
                ON t.orderid = s.orderid
                WHEN MATCHED THEN UPDATE SET
                    customerid = s.customerid,
                    orderdate = s.orderdate,
                    totalamount = s.totalamount,
                    customer_name = s.customer_name,
                    customer_region = s.customer_region,
                    processing_timestamp = s.processing_timestamp
                WHEN NOT MATCHED THEN INSERT (
                    orderid, customerid, orderdate, totalamount,
                    customer_name, customer_region, event_timestamp,
                    processing_timestamp
                ) VALUES (
                    s.orderid, s.customerid, s.orderdate, s.totalamount,
                    s.customer_name, s.customer_region, s.event_timestamp,
                    s.processing_timestamp
                )
            """,
            'products': f"""
                MERGE INTO products_enriched t
                USING {temp_table} s
                ON t.productid = s.productid
                WHEN MATCHED THEN UPDATE SET
                    name = s.name,
                    price = s.price,
                    stocklevel = s.stocklevel,
                    processing_timestamp = s.processing_timestamp
                WHEN NOT MATCHED THEN INSERT (
                    productid, name, price, stocklevel,
                    event_timestamp, processing_timestamp
                ) VALUES (
                    s.productid, s.name, s.price, s.stocklevel,
                    s.event_timestamp, s.processing_timestamp
                )
            """,
            'customers': f"""
                MERGE INTO customers_enriched t
                USING {temp_table} s
                ON t.customerid = s.customerid
                WHEN MATCHED THEN UPDATE SET
                    name = s.name,
                    region = s.region,
                    processing_timestamp = s.processing_timestamp
                WHEN NOT MATCHED THEN INSERT (
                    customerid, name, region, event_timestamp,
                    processing_timestamp
                ) VALUES (
                    s.customerid, s.name, s.region,
                    s.event_timestamp, s.processing_timestamp
                )
            """
        }
        
        # Execute MERGE
        query = merge_queries[table_name]
        execute_athena_query(query)
        print(f"Merged {table_name}")
        
    finally:
        # Clean up temp table
        try:
            execute_athena_query(f'DROP TABLE IF EXISTS {temp_table}')
        except:
            pass


def lambda_handler(event, context):
    """Process Bronze files and merge to Silver Iceberg tables"""
    
    # Read Bronze Parquet files
    bronze_records = []
    for s3_record in event['Records']:
        bucket = s3_record['s3']['bucket']['name']
        key = unquote_plus(s3_record['s3']['object']['key'])
        
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        table = pq.read_table(io.BytesIO(obj['Body'].read()))
        bronze_records.extend(table.to_pylist())
    
    if not bronze_records:
        return {'statusCode': 200}
    
    # Transform with enrichment
    silver_data = transform_records(bronze_records)
    
    # Write staging and merge
    for table_name, records in silver_data.items():
        if not records:
            continue
        
        # Write to temp staging location
        staging_key = f"silver/staging/{table_name}/{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.parquet"
        table = pa.Table.from_pylist(records)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        s3_client.put_object(Bucket=S3_BUCKET, Key=staging_key, Body=buffer.getvalue())
        
        # MERGE into Iceberg
        staging_path = f"s3://{S3_BUCKET}/{staging_key}"
        merge_to_iceberg(table_name, staging_path)
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Processed {len(bronze_records)} records')
    }