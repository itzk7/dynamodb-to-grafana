# Lambda Layer for shared dependencies
resource "aws_lambda_layer_version" "python_deps" {
  filename            = "${path.module}/../lambda/layers/python_deps.zip"
  layer_name          = "${var.project_name}-python-deps-${var.environment}"
  compatible_runtimes = ["python3.11"]
  description         = "Shared Python dependencies (pyarrow)"

  # This will be created by the build script
  lifecycle {
    create_before_destroy = true
  }
}

# Bronze Layer Lambda Function
resource "aws_lambda_function" "bronze_processor" {
  filename         = "${path.module}/../lambda/bronze/deployment.zip"
  function_name    = "${var.project_name}-bronze-processor-${var.environment}"
  role            = aws_iam_role.bronze_lambda_role.arn
  handler         = "handler.lambda_handler"
  runtime         = "python3.11"
  timeout         = 300
  memory_size     = 512
  
  source_code_hash = fileexists("${path.module}/../lambda/bronze/deployment.zip") ? filebase64sha256("${path.module}/../lambda/bronze/deployment.zip") : null

  layers = [aws_lambda_layer_version.python_deps.arn]

  environment {
    variables = {
      S3_BUCKET           = aws_s3_bucket.data_lake.bucket
      S3_PREFIX           = "bronze"
      ENVIRONMENT         = var.environment
    }
  }

  tags = {
    Name = "Bronze Processor Lambda"
  }

  lifecycle {
    create_before_destroy = true
  }
}

# Event Source Mapping - Orders Stream
resource "aws_lambda_event_source_mapping" "orders_stream" {
  event_source_arn  = aws_dynamodb_table.orders.stream_arn
  function_name     = aws_lambda_function.bronze_processor.arn
  starting_position = "LATEST"
  batch_size        = 100
  
  maximum_batching_window_in_seconds = 10

  filter_criteria {
    filter {
      pattern = jsonencode({
        eventName = ["INSERT", "MODIFY"]
      })
    }
  }
}

# Event Source Mapping - Products Stream
resource "aws_lambda_event_source_mapping" "products_stream" {
  event_source_arn  = aws_dynamodb_table.products.stream_arn
  function_name     = aws_lambda_function.bronze_processor.arn
  starting_position = "LATEST"
  batch_size        = 100
  
  maximum_batching_window_in_seconds = 10

  filter_criteria {
    filter {
      pattern = jsonencode({
        eventName = ["INSERT", "MODIFY"]
      })
    }
  }
}

# Event Source Mapping - Customers Stream
resource "aws_lambda_event_source_mapping" "customers_stream" {
  event_source_arn  = aws_dynamodb_table.customers.stream_arn
  function_name     = aws_lambda_function.bronze_processor.arn
  starting_position = "LATEST"
  batch_size        = 100
  
  maximum_batching_window_in_seconds = 10

  filter_criteria {
    filter {
      pattern = jsonencode({
        eventName = ["INSERT", "MODIFY"]
      })
    }
  }
}

# Silver Layer Lambda Function
resource "aws_lambda_function" "silver_processor" {
  filename         = "${path.module}/../lambda/silver/deployment.zip"
  function_name    = "${var.project_name}-silver-processor-${var.environment}"
  role            = aws_iam_role.silver_lambda_role.arn
  handler         = "handler.lambda_handler"
  runtime         = "python3.11"
  timeout         = 900
  memory_size     = 1024
  
  source_code_hash = fileexists("${path.module}/../lambda/silver/deployment.zip") ? filebase64sha256("${path.module}/../lambda/silver/deployment.zip") : null

  layers = [aws_lambda_layer_version.python_deps.arn]

  environment {
    variables = {
      S3_BUCKET              = aws_s3_bucket.data_lake.bucket
      BRONZE_PREFIX          = "bronze"
      SILVER_PREFIX          = "silver"
      ORDERS_TABLE           = aws_dynamodb_table.orders.name
      PRODUCTS_TABLE         = aws_dynamodb_table.products.name
      CUSTOMERS_TABLE        = aws_dynamodb_table.customers.name
      ATHENA_DATABASE        = aws_glue_catalog_database.silver.name  # Silver database for MERGE INTO
      ATHENA_WORKGROUP       = aws_athena_workgroup.main.name
      ATHENA_OUTPUT_LOCATION = "s3://${aws_s3_bucket.athena_results.bucket}/silver-processing/"
      ENVIRONMENT            = var.environment
    }
  }

  tags = {
    Name = "Silver Processor Lambda"
  }

  lifecycle {
    create_before_destroy = true
  }
}

# EventBridge Rule for Silver Layer Processing (Every 30 minutes)
resource "aws_cloudwatch_event_rule" "silver_processing_schedule" {
  name                = "${var.project_name}-silver-processing-${var.environment}"
  description         = "Trigger silver layer processing every 30 minutes"
  schedule_expression = "rate(30 minutes)"

  tags = {
    Name = "Silver Processing Schedule"
  }
}

resource "aws_cloudwatch_event_target" "silver_processing_target" {
  rule      = aws_cloudwatch_event_rule.silver_processing_schedule.name
  target_id = "SilverProcessorLambda"
  arn       = aws_lambda_function.silver_processor.arn
}

resource "aws_lambda_permission" "allow_eventbridge_invoke_silver" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.silver_processor.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.silver_processing_schedule.arn
}

# Gold Layer Lambda Function
resource "aws_lambda_function" "gold_processor" {
  filename         = "${path.module}/../lambda/gold/deployment.zip"
  function_name    = "${var.project_name}-gold-processor-${var.environment}"
  role            = aws_iam_role.gold_lambda_role.arn
  handler         = "handler.lambda_handler"
  runtime         = "python3.11"
  timeout         = 900
  memory_size     = 2048
  
  source_code_hash = fileexists("${path.module}/../lambda/gold/deployment.zip") ? filebase64sha256("${path.module}/../lambda/gold/deployment.zip") : null

  layers = [aws_lambda_layer_version.python_deps.arn]

  environment {
    variables = {
      S3_BUCKET              = aws_s3_bucket.data_lake.bucket
      SILVER_PREFIX          = "silver"
      GOLD_PREFIX            = "gold"
      SILVER_DATABASE        = aws_glue_catalog_database.silver.name
      GOLD_DATABASE          = aws_glue_catalog_database.gold.name
      ATHENA_WORKGROUP       = aws_athena_workgroup.main.name
      ATHENA_OUTPUT_LOCATION = "s3://${aws_s3_bucket.athena_results.bucket}/gold-processing/"
      REFRESH_DAYS           = "3"
      ENVIRONMENT            = var.environment
    }
  }

  tags = {
    Name = "Gold Processor Lambda"
  }

  lifecycle {
    create_before_destroy = true
  }
}

# EventBridge Rule for Gold Layer Processing (Hourly)
resource "aws_cloudwatch_event_rule" "gold_processing_schedule" {
  name                = "${var.project_name}-gold-processing-${var.environment}"
  description         = "Trigger gold layer processing hourly"
  schedule_expression = "rate(1 hour)"

  tags = {
    Name = "Gold Processing Schedule"
  }
}

resource "aws_cloudwatch_event_target" "gold_processing_target" {
  rule      = aws_cloudwatch_event_rule.gold_processing_schedule.name
  target_id = "GoldProcessorLambda"
  arn       = aws_lambda_function.gold_processor.arn
}

resource "aws_lambda_permission" "allow_eventbridge_invoke_gold" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.gold_processor.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.gold_processing_schedule.arn
}
