output "dynamodb_tables" {
  description = "DynamoDB table names"
  value = {
    orders    = aws_dynamodb_table.orders.name
    products  = aws_dynamodb_table.products.name
    customers = aws_dynamodb_table.customers.name
  }
}

output "dynamodb_stream_arns" {
  description = "DynamoDB Stream ARNs"
  value = {
    orders    = aws_dynamodb_table.orders.stream_arn
    products  = aws_dynamodb_table.products.stream_arn
    customers = aws_dynamodb_table.customers.stream_arn
  }
}

output "s3_bucket" {
  description = "S3 data lake bucket name"
  value       = aws_s3_bucket.data_lake.bucket
}

output "glue_databases" {
  description = "Glue catalog database names"
  value = {
    bronze = aws_glue_catalog_database.bronze.name
    silver = aws_glue_catalog_database.silver.name
    gold   = aws_glue_catalog_database.gold.name
  }
}

output "athena_workgroup" {
  description = "Athena workgroup name"
  value       = aws_athena_workgroup.main.name
}

output "lambda_functions" {
  description = "Lambda function names"
  value = {
    bronze = aws_lambda_function.bronze_processor.function_name
    silver = aws_lambda_function.silver_processor.function_name
    gold   = aws_lambda_function.gold_processor.function_name
  }
}




