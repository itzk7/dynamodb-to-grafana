# S3 Data Lake Bucket
resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.s3_bucket_prefix}-${var.project_name}-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name = "Data Lake Bucket"
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "bronze-transition"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 180
      storage_class = "GLACIER"
    }
  }

  rule {
    id     = "silver-transition"
    status = "Enabled"

    filter {
      prefix = "silver/"
    }

    transition {
      days          = 180
      storage_class = "STANDARD_IA"
    }
  }

  rule {
    id     = "gold-long-term"
    status = "Enabled"

    filter {
      prefix = "gold/"
    }

    transition {
      days          = 365
      storage_class = "STANDARD_IA"
    }
  }
}

# S3 Bucket for Athena Query Results
resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.s3_bucket_prefix}-athena-results-${data.aws_caller_identity.current.account_id}"

  tags = {
    Name = "Athena Query Results"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  rule {
    id     = "cleanup-old-results"
    status = "Enabled"

    filter {
      prefix = ""
    }

    expiration {
      days = 30
    }
  }
}

# Bucket policy to allow Athena workgroup and Lambda roles to access
resource "aws_s3_bucket_policy" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowAthenaWorkgroupAccess"
        Effect = "Allow"
        Principal = {
          AWS = [
            aws_iam_role.gold_lambda_role.arn,
            aws_iam_role.silver_lambda_role.arn
          ]
        }
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.athena_results.arn,
          "${aws_s3_bucket.athena_results.arn}/*"
        ]
      }
    ]
  })
}




