# Athena Workgroup - Required for query execution
resource "aws_athena_workgroup" "main" {
  name          = "${var.project_name}-workgroup-${var.environment}"
  force_destroy = true
  
  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/results/"
    }

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    # Allow queries to override output location (don't enforce workgroup config)
    enforce_workgroup_configuration = false
  }

  tags = {
    Name = "Main Athena Workgroup"
  }
}

# Optional: Sample query for daily sales (for demo purposes)
resource "aws_athena_named_query" "daily_sales" {
  name      = "daily_sales_example"
  workgroup = aws_athena_workgroup.main.id
  database  = aws_glue_catalog_database.gold.name
  query     = <<-EOT
    SELECT 
      order_date,
      region,
      SUM(total_sales) as total_sales,
      SUM(order_count) as order_count
    FROM daily_sales_metrics
    ORDER BY order_date DESC
    LIMIT 30;
  EOT

  description = "Example: Daily sales by region"
}
