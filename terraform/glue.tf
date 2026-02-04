# Glue Database - Bronze Layer
resource "aws_glue_catalog_database" "bronze" {
  name = "${var.project_name}-bronze-${var.environment}"

  description = "Bronze layer - Raw data from DynamoDB Streams"
}

# Glue Database - Silver Layer
resource "aws_glue_catalog_database" "silver" {
  name = "${var.project_name}-silver-${var.environment}"

  description = "Silver layer - Cleansed, deduplicated, and enriched data"
}

# Glue Database - Gold Layer
resource "aws_glue_catalog_database" "gold" {
  name = "${var.project_name}-gold-${var.environment}"

  description = "Gold layer - Aggregated business metrics and KPIs"
}

# All crawlers removed - using explicit table schemas
# See tables.tf for all layer definitions




