"""
Silver Layer: Scheduled enrichment with Iceberg MERGE
Runs every 30 minutes, scans unprocessed Bronze files, 
transforms and merges into Silver Iceberg tables.
"""
import json
import os
import boto3
from datetime import datetime, timedelta
import time
import io
import uuid
import pyarrow as pa
import pyarrow.parquet as pq

s3_client = boto3.client('s3')
athena_client = boto3.client('athena')
dynamodb = boto3.resource('dynamodb')

S3_BUCKET = os.environ['S3_BUCKET']
CUSTOMERS_TABLE = os.environ['CUSTOMERS_TABLE']
ORDERS_TABLE = os.environ['ORDERS_TABLE']
PRODUCTS_TABLE = os.environ['PRODUCTS_TABLE']
ATHENA_DATABASE = os.environ['ATHENA_DATABASE']
ATHENA_WORKGROUP = os.environ['ATHENA_WORKGROUP']
ATHENA_OUTPUT = os.environ['ATHENA_OUTPUT_LOCATION']

WATERMARK_KEY = "silver/watermark.json"
BRONZE_TABLES = [ORDERS_TABLE, PRODUCTS_TABLE, CUSTOMERS_TABLE]


def get_watermark():
    """Read the last processed timestamp from S3"""
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=WATERMARK_KEY)
        data = json.loads(obj['Body'].read().decode('utf-8'))
        return datetime.fromisoformat(data['last_processed'])
    except s3_client.exceptions.NoSuchKey:
        # First run — process last 24 hours
        return datetime.utcnow() - timedelta(hours=24)
    except Exception as e:
        print(f"Warning: Could not read watermark: {e}")
        return datetime.utcnow() - timedelta(hours=1)


def save_watermark(timestamp):
    """Save the processing watermark to S3"""
    data = {
        'last_processed': timestamp.isoformat(),
        'updated_at': datetime.utcnow().isoformat()
    }
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=WATERMARK_KEY,
        Body=json.dumps(data).encode('utf-8')
    )
    print(f"  Watermark updated: {timestamp.isoformat()}")


def scan_bronze_files(since):
    """List all Bronze parquet files modified after the watermark"""
    new_files = []

    for table_name in BRONZE_TABLES:
        prefix = f"bronze/{table_name}/"
        paginator = s3_client.get_paginator('list_objects_v2')

        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('.parquet') and obj['LastModified'].replace(tzinfo=None) > since:
                    new_files.append(obj['Key'])

    return new_files


def read_bronze_files(file_keys):
    """Read and combine all Bronze parquet files"""
    all_records = []

    for key in file_keys:
        try:
            obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
            table = pq.read_table(io.BytesIO(obj['Body'].read()))
            all_records.extend(table.to_pylist())
        except Exception as e:
            print(f"  Warning: Could not read {key}: {e}")

    return all_records

def execute_athena_query(query):
    """Execute an Athena query and wait for completion"""
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT},
        WorkGroup=ATHENA_WORKGROUP
    )
    
    query_id = response['QueryExecutionId']
    
    for _ in range(120):  # up to 4 minutes
        status = athena_client.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']

        if state == 'SUCCEEDED':
            return query_id
        elif state in ['FAILED', 'CANCELLED']:
            reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
            raise Exception(f"Query {state}: {reason}")

        time.sleep(2)
    
    raise Exception("Query timeout after 4 minutes")

def enrich_order_data(order):
    """Enrich orders with customer data from DynamoDB"""
    customer_id = order.get('CustomerID')
    if not customer_id:
        return order
    
    table = dynamodb.Table(CUSTOMERS_TABLE)
    try:
        response = table.get_item(Key={'CustomerID': customer_id})
        if 'Item' in response:
            customer = response['Item']
            order['customer_name'] = customer.get('Name', '')
            order['customer_region'] = customer.get('Region', '')
    except Exception as e:
        print(f"  Warning: Could not enrich customer {customer_id}: {e}")
    
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


SCHEMA_DEFS = {
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

ICEBERG_DDL = {
    'orders': """
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
        LOCATION 's3://{bucket}/silver/iceberg/orders/'
        TBLPROPERTIES ('table_type'='ICEBERG')
    """,
    'products': """
        CREATE TABLE IF NOT EXISTS products_enriched (
            productid STRING,
            name STRING,
            price DOUBLE,
            stocklevel INT,
            event_timestamp STRING,
            processing_timestamp STRING
        )
        LOCATION 's3://{bucket}/silver/iceberg/products/'
        TBLPROPERTIES ('table_type'='ICEBERG')
    """,
    'customers': """
        CREATE TABLE IF NOT EXISTS customers_enriched (
            customerid STRING,
            name STRING,
            region STRING,
            event_timestamp STRING,
            processing_timestamp STRING
        )
        LOCATION 's3://{bucket}/silver/iceberg/customers/'
        TBLPROPERTIES ('table_type'='ICEBERG')
    """
}

MERGE_SQL = {
    'orders': """
        MERGE INTO orders_enriched t
        USING {temp} s
        ON t.orderid = s.orderid
        WHEN MATCHED THEN UPDATE SET
            customerid = s.customerid,
            orderdate = s.orderdate,
            totalamount = s.totalamount,
            customer_name = s.customer_name,
            customer_region = s.customer_region,
            event_timestamp = s.event_timestamp,
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
    'products': """
        MERGE INTO products_enriched t
        USING {temp} s
        ON t.productid = s.productid
        WHEN MATCHED THEN UPDATE SET
            name = s.name,
            price = s.price,
            stocklevel = s.stocklevel,
            event_timestamp = s.event_timestamp,
            processing_timestamp = s.processing_timestamp
        WHEN NOT MATCHED THEN INSERT (
            productid, name, price, stocklevel,
            event_timestamp, processing_timestamp
        ) VALUES (
            s.productid, s.name, s.price, s.stocklevel,
            s.event_timestamp, s.processing_timestamp
        )
    """,
    'customers': """
        MERGE INTO customers_enriched t
        USING {temp} s
        ON t.customerid = s.customerid
        WHEN MATCHED THEN UPDATE SET
            name = s.name,
            region = s.region,
            event_timestamp = s.event_timestamp,
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

PRIMARY_KEYS = {
    'orders': 'orderid',
    'products': 'productid',
    'customers': 'customerid'
}


def merge_to_iceberg(table_name, records):
    """
    Write records to staging, create temp table, MERGE into Iceberg.
    Single invocation — no concurrency issues.
    """
    invocation_id = uuid.uuid4().hex[:12]
    temp_table = f"temp_{table_name}_{invocation_id}"
    staging_key = f"silver/staging/{table_name}/{invocation_id}/data.parquet"

    try:
        # 1. Deduplicate within batch (keep latest per primary key)
        pk = PRIMARY_KEYS[table_name]
        seen = {}
        for record in records:
            seen[record[pk]] = record
        deduped = list(seen.values())
        print(f"    {len(records)} records → {len(deduped)} after dedup")

        # 2. Write staging parquet to S3
        arrow_table = pa.Table.from_pylist(deduped)
        buffer = io.BytesIO()
        pq.write_table(arrow_table, buffer)
        s3_client.put_object(Bucket=S3_BUCKET, Key=staging_key, Body=buffer.getvalue())

        # 3. Create temp external table pointing to staging
        staging_dir = f"s3://{S3_BUCKET}/silver/staging/{table_name}/{invocation_id}/"
        execute_athena_query(f"""
            CREATE EXTERNAL TABLE {temp_table} (
                {SCHEMA_DEFS[table_name]}
            )
            STORED AS PARQUET
            LOCATION '{staging_dir}'
        """)

        # 4. Ensure Iceberg table exists
        execute_athena_query(ICEBERG_DDL[table_name].format(bucket=S3_BUCKET))

        # 5. MERGE — single execution, no concurrency, no conflict
        execute_athena_query(MERGE_SQL[table_name].format(temp=temp_table))
        print(f"    ✓ Merged into {table_name}_enriched")

    finally:
        # Clean up temp table and staging
        try:
            execute_athena_query(f'DROP TABLE IF EXISTS {temp_table}')
        except Exception:
            pass
        try:
            s3_client.delete_object(Bucket=S3_BUCKET, Key=staging_key)
        except Exception:
            pass


def lambda_handler(event, context):
    """
    Scheduled Silver Layer processor.
    Triggered every 30 minutes by EventBridge.
    Scans Bronze for new files, transforms, and merges into Iceberg.
    """
    # 1. Get watermark (last processed time)
    watermark = get_watermark()
    run_start = datetime.utcnow()
    print(f"\nWatermark: {watermark.isoformat()}")
    print(f"Run start: {run_start.isoformat()}")
    print(f"Scanning for files since watermark...")

    # 2. Scan for new Bronze files
    new_files = scan_bronze_files(watermark)

    if not new_files:
        print("No new Bronze files found. Nothing to process.")
        save_watermark(run_start)
        return {'statusCode': 200, 'body': 'No new files'}

    print(f"Found {len(new_files)} new Bronze file(s)")
    for f in new_files:
        print(f"  • {f}")

    # 3. Read all new Bronze files
    bronze_records = read_bronze_files(new_files)
    print(f"\nRead {len(bronze_records)} total records")
    
    if not bronze_records:
        save_watermark(run_start)
        return {'statusCode': 200, 'body': 'No records in files'}
    
    # 4. Transform with enrichment
    silver_data = transform_records(bronze_records)
    
    # 5. MERGE each table into Iceberg
    for table_name, records in silver_data.items():
        if not records:
            continue
        
        print(f"\n  📊 {table_name}:")
        merge_to_iceberg(table_name, records)

    # 6. Update watermark
    save_watermark(run_start)

    print(f"  Files processed: {len(new_files)}")
    print(f"  Records processed: {len(bronze_records)}")

    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'files_processed': len(new_files),
            'records_processed': len(bronze_records)
        })
    }
