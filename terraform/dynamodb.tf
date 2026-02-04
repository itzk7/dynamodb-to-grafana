# Orders Table
resource "aws_dynamodb_table" "orders" {
  name           = "${var.project_name}-orders-${var.environment}"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "OrderID"
  stream_enabled = true
  stream_view_type = "NEW_AND_OLD_IMAGES"

  attribute {
    name = "OrderID"
    type = "S"
  }

  attribute {
    name = "CustomerID"
    type = "S"
  }

  attribute {
    name = "OrderDate"
    type = "S"
  }

  global_secondary_index {
    name            = "CustomerIndex"
    hash_key        = "CustomerID"
    range_key       = "OrderDate"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = {
    Name = "Orders Table"
  }
}

# Products Table
resource "aws_dynamodb_table" "products" {
  name           = "${var.project_name}-products-${var.environment}"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "ProductID"
  stream_enabled = true
  stream_view_type = "NEW_AND_OLD_IMAGES"

  attribute {
    name = "ProductID"
    type = "S"
  }

  attribute {
    name = "Category"
    type = "S"
  }

  global_secondary_index {
    name            = "CategoryIndex"
    hash_key        = "Category"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = {
    Name = "Products Table"
  }
}

# Customers Table
resource "aws_dynamodb_table" "customers" {
  name           = "${var.project_name}-customers-${var.environment}"
  billing_mode   = "PAY_PER_REQUEST"
  hash_key       = "CustomerID"
  stream_enabled = true
  stream_view_type = "NEW_AND_OLD_IMAGES"

  attribute {
    name = "CustomerID"
    type = "S"
  }

  attribute {
    name = "Region"
    type = "S"
  }

  global_secondary_index {
    name            = "RegionIndex"
    hash_key        = "Region"
    projection_type = "ALL"
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = {
    Name = "Customers Table"
  }
}




