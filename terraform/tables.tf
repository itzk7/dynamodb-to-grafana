# BRONZE LAYER - Raw CDC Events
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
