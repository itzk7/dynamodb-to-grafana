"""
Bronze Layer: Capture raw DynamoDB changes via Streams
Writes CDC events to S3 as Parquet files
"""
import json
import os
import boto3
from datetime import datetime
import io
import uuid
import pyarrow as pa
import pyarrow.parquet as pq

s3_client = boto3.client('s3')

S3_BUCKET = os.environ['S3_BUCKET']
S3_PREFIX = os.environ.get('S3_PREFIX', 'bronze')


def flatten_dynamodb_item(item):
    """Convert DynamoDB item format to simple dict"""
    result = {}
    for key, value in item.items():
        # Handle different DynamoDB types
        if 'S' in value:
            result[key] = value['S']
        elif 'N' in value:
            result[key] = float(value['N'])
        elif 'BOOL' in value:
            result[key] = value['BOOL']
        elif 'L' in value:
            result[key] = [list(item.values())[0] for item in value['L']]
        elif 'NULL' in value:
            result[key] = None
    return result


def extract_table_name(event_source_arn):
    """Get table name from DynamoDB Stream ARN"""
    # ARN format: arn:aws:dynamodb:region:account:table/TableName/stream/...
    return event_source_arn.split('/')[1]


def lambda_handler(event, context):
    """Process DynamoDB Stream events and write to S3"""
    records = []
    
    for record in event['Records']:
        # Extract key CDC fields
        table_name = extract_table_name(record['eventSourceARN'])
        event_name = record['eventName']  # INSERT, MODIFY, REMOVE
        
        # Get the new item state (skip REMOVE events for simplicity)
        if event_name == 'REMOVE' or 'NewImage' not in record['dynamodb']:
            continue
            
        new_image = flatten_dynamodb_item(record['dynamodb']['NewImage'])
        
        records.append({
            '_event_name': event_name,
            '_event_timestamp': datetime.utcnow().isoformat(),
            '_table_name': table_name,
            **new_image  # Include all the actual data
        })
    
    if not records:
        return {'statusCode': 200, 'body': 'No records to process'}
    
    # Partition by processing time with UUID to prevent same-second collisions
    now = datetime.utcnow()
    table_name = records[0]['_table_name']
    batch_id = uuid.uuid4().hex[:8]
    s3_key = f"{S3_PREFIX}/{table_name}/year={now.year}/month={now.month:02d}/day={now.day:02d}/{now.strftime('%Y%m%d%H%M%S')}_{batch_id}.parquet"
    print(f"Writing batch_id={batch_id}, records={len(records)}, key={s3_key}")
    
    # Write to S3 as Parquet
    table = pa.Table.from_pylist(records)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression='snappy')
    
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue()
    )
    
    print(f"Wrote {len(records)} records to s3://{S3_BUCKET}/{s3_key}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Processed {len(records)} records')
    }
