data "aws_iam_policy_document" "emr_service_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["elasticmapreduce.amazonaws.com"]
    }
  }
}

data "aws_iam_policy_document" "emr_ec2_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "emr_service_role" {
  name               = "${var.project_name}-emr-service-role"
  assume_role_policy = data.aws_iam_policy_document.emr_service_assume_role.json

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_iam_role_policy_attachment" "emr_service_policy" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEMRServicePolicy_v2"
}

resource "aws_iam_role" "emr_ec2_role" {
  name               = "${var.project_name}-emr-ec2-role"
  assume_role_policy = data.aws_iam_policy_document.emr_ec2_assume_role.json

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_iam_role_policy_attachment" "emr_worker_policy" {
  role       = aws_iam_role.emr_ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEMRWorkerInstancePolicy"
}

data "aws_iam_policy_document" "emr_s3_access" {
  statement {
    sid    = "AllowLakehouseS3"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
      "s3:GetBucketLocation",
    ]
    resources = [
      aws_s3_bucket.raw.arn,
      "${aws_s3_bucket.raw.arn}/*",
      aws_s3_bucket.curated.arn,
      "${aws_s3_bucket.curated.arn}/*",
      aws_s3_bucket.logs.arn,
      "${aws_s3_bucket.logs.arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "emr_s3_access" {
  name   = "${var.project_name}-emr-s3-access"
  role   = aws_iam_role.emr_ec2_role.id
  policy = data.aws_iam_policy_document.emr_s3_access.json
}

resource "aws_iam_instance_profile" "emr" {
  name = "${var.project_name}-emr-instance-profile"
  role = aws_iam_role.emr_ec2_role.name
}

resource "aws_emr_cluster" "lakehouse" {
  name          = "${var.project_name}-spark"
  release_label = var.emr_release_label
  applications  = ["Spark", "Hive", "Livy"]

  master_instance_group {
    instance_type = var.emr_master_instance_type
  }

  core_instance_group {
    instance_type  = var.emr_core_instance_type
    instance_count = var.emr_core_instance_count

    ebs_config {
      size                 = 500
      type                 = "gp3"
      volumes_per_instance = 1
    }
  }

  ec2_attributes {
    subnet_id            = var.subnet_ids[0]
    instance_profile     = aws_iam_instance_profile.emr.arn
  }

  service_role = aws_iam_role.emr_service_role.arn

  bootstrap_action {
    path = "s3://${var.s3_logs_bucket_name}/bootstrap/install-delta-iceberg.sh"
    name = "Install Delta Lake and Iceberg JARs"
  }

  configurations_json = jsonencode([
    {
      Classification = "spark-defaults"
      Properties = {
        "spark.sql.extensions"                             = "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        "spark.sql.catalog.spark_catalog"                  = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        "spark.sql.catalog.iceberg"                        = "org.apache.iceberg.spark.SparkCatalog"
        "spark.sql.catalog.iceberg.type"                   = "rest"
        "spark.sql.catalog.iceberg.uri"                    = "http://iceberg-rest-catalog:8181"
        "spark.hadoop.fs.s3a.impl"                         = "org.apache.hadoop.fs.s3a.S3AFileSystem"
        "spark.hadoop.fs.s3a.fast.upload"                  = "true"
        "spark.hadoop.fs.s3a.multipart.size"               = "128M"
        "spark.hadoop.fs.s3a.connection.maximum"           = "100"
        "spark.executor.memory"                            = "6g"
        "spark.executor.memoryOverhead"                    = "1g"
        "spark.driver.memory"                              = "4g"
        "spark.sql.shuffle.partitions"                     = "200"
        "spark.serializer"                                 = "org.apache.spark.serializer.KryoSerializer"
        "spark.sql.adaptive.enabled"                       = "true"
        "spark.sql.adaptive.coalescePartitions.enabled"    = "true"
      }
    },
    {
      Classification = "spark-env"
      Configurations = [
        {
          Classification = "export"
          Properties = {
            "PYSPARK_PYTHON" = "/usr/bin/python3"
          }
        }
      ]
    }
  ])

  auto_termination_policy {
    idle_timeout = 3600
  }

  log_uri = "s3://${var.s3_logs_bucket_name}/emr-logs/"

  tags = {
    Name        = "${var.project_name}-spark"
    Environment = var.environment
    Project     = var.project_name
  }
}
