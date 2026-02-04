# ============================================================================
# Explicit Table Definitions for All Layers (No Crawlers)
# All tables use partitioned Parquet format for optimal performance
# ============================================================================

# ============================================================================
# BRONZE LAYER - Raw CDC Events
# ============================================================================

resource "aws_glue_catalog_table" "bronze_orders" {
  database_name = aws_glue_catalog_database.bronze.name
  name          = "orders"
  table_type    = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/orders/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "event_id"
      type = "string"
    }

    columns {
      name = "event_name"
      type = "string"
    }

    columns {
      name = "event_timestamp"
      type = "string"
    }

    columns {
      name = "table_name"
      type = "string"
    }

    columns {
      name = "keys_json"
      type = "string"
    }

    columns {
      name = "new_image_json"
      type = "string"
    }

    columns {
      name = "processing_timestamp"
      type = "string"
    }
  }

  partition_keys {
    name = "year"
    type = "string"
  }

  partition_keys {
    name = "month"
    type = "string"
  }

  partition_keys {
    name = "day"
    type = "string"
  }

  parameters = {
    "projection.enabled"     = "true"
    "projection.year.type"   = "integer"
    "projection.year.range"  = "2020,2030"
    "projection.month.type"  = "integer"
    "projection.month.range" = "1,12"
    "projection.month.digits" = "2"
    "projection.day.type"    = "integer"
    "projection.day.range"   = "1,31"
    "projection.day.digits"  = "2"
    "storage.location.template" = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/orders/year=$${year}/month=$${month}/day=$${day}"
  }
}

resource "aws_glue_catalog_table" "bronze_products" {
  database_name = aws_glue_catalog_database.bronze.name
  name          = "products"
  table_type    = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/products/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "event_id"
      type = "string"
    }

    columns {
      name = "event_name"
      type = "string"
    }

    columns {
      name = "event_timestamp"
      type = "string"
    }

    columns {
      name = "table_name"
      type = "string"
    }

    columns {
      name = "keys_json"
      type = "string"
    }

    columns {
      name = "new_image_json"
      type = "string"
    }

    columns {
      name = "processing_timestamp"
      type = "string"
    }
  }

  partition_keys {
    name = "year"
    type = "string"
  }

  partition_keys {
    name = "month"
    type = "string"
  }

  partition_keys {
    name = "day"
    type = "string"
  }

  parameters = {
    "projection.enabled"     = "true"
    "projection.year.type"   = "integer"
    "projection.year.range"  = "2020,2030"
    "projection.month.type"  = "integer"
    "projection.month.range" = "1,12"
    "projection.month.digits" = "2"
    "projection.day.type"    = "integer"
    "projection.day.range"   = "1,31"
    "projection.day.digits"  = "2"
    "storage.location.template" = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/products/year=$${year}/month=$${month}/day=$${day}"
  }
}

resource "aws_glue_catalog_table" "bronze_customers" {
  database_name = aws_glue_catalog_database.bronze.name
  name          = "customers"
  table_type    = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/customers/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "event_id"
      type = "string"
    }

    columns {
      name = "event_name"
      type = "string"
    }

    columns {
      name = "event_timestamp"
      type = "string"
    }

    columns {
      name = "table_name"
      type = "string"
    }

    columns {
      name = "keys_json"
      type = "string"
    }

    columns {
      name = "new_image_json"
      type = "string"
    }

    columns {
      name = "processing_timestamp"
      type = "string"
    }
  }

  partition_keys {
    name = "year"
    type = "string"
  }

  partition_keys {
    name = "month"
    type = "string"
  }

  partition_keys {
    name = "day"
    type = "string"
  }

  parameters = {
    "projection.enabled"     = "true"
    "projection.year.type"   = "integer"
    "projection.year.range"  = "2020,2030"
    "projection.month.type"  = "integer"
    "projection.month.range" = "1,12"
    "projection.month.digits" = "2"
    "projection.day.type"    = "integer"
    "projection.day.range"   = "1,31"
    "projection.day.digits"  = "2"
    "storage.location.template" = "s3://${aws_s3_bucket.data_lake.bucket}/bronze/customers/year=$${year}/month=$${month}/day=$${day}"
  }
}

# ============================================================================
# SILVER LAYER - Cleansed and Enriched Data (Created dynamically by Lambda)
# ============================================================================
# 
# Silver tables are created dynamically by the silver lambda using MERGE operations:
#   - orders_enriched: Iceberg table with MERGE deduplication
#   - products_enriched: Iceberg table with MERGE deduplication  
#   - customers_enriched: Iceberg table with MERGE deduplication
#
# The MERGE command automatically creates proper Iceberg tables with metadata,
# so no Terraform definitions needed. Attempting to pre-create them via Terraform
# causes "Iceberg type table without metadata location" errors.

# ============================================================================
# GOLD LAYER - Aggregated Metrics (Created dynamically by Lambda)
# ============================================================================
# 
# Gold tables are created dynamically by the gold lambda using CTAS queries:
#   - daily_sales_by_region: Delta refresh (90 days history, updates last 3 days)
#   - product_performance: Full refresh each run (small dataset)
#   - key_metrics: Full refresh (current KPIs: today, 7d, 30d, 90d)
#
# This approach is simpler for demos/blog posts and avoids Iceberg complexity
# while still demonstrating medallion architecture and aggregation patterns.
