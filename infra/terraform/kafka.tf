resource "aws_security_group" "msk" {
  name        = "${var.project_name}-msk-sg"
  description = "Security group for MSK brokers"
  vpc_id      = var.vpc_id

  ingress {
    description = "Kafka plaintext"
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  ingress {
    description = "Kafka TLS"
    from_port   = 9094
    to_port     = 9094
    protocol    = "tcp"
    cidr_blocks = var.allowed_cidr_blocks
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "${var.project_name}-msk-sg"
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_msk_configuration" "lakehouse" {
  name              = "${var.project_name}-msk-config"
  kafka_versions    = [var.kafka_kafka_version]
  description       = "MSK broker configuration for the lakehouse pipeline"

  server_properties = <<-PROPERTIES
    auto.create.topics.enable=false
    log.retention.hours=168
    num.partitions=6
    default.replication.factor=3
    min.insync.replicas=2
    log.segment.bytes=1073741824
    log.retention.check.interval.ms=300000
    compression.type=lz4
  PROPERTIES
}

resource "aws_msk_cluster" "lakehouse" {
  cluster_name           = "${var.project_name}-kafka"
  kafka_version          = var.kafka_kafka_version
  number_of_broker_nodes = var.kafka_number_of_broker_nodes

  broker_node_group_info {
    instance_type   = var.kafka_instance_type
    client_subnets  = var.subnet_ids

    storage_info {
      ebs_storage_info {
        volume_size = 1000
      }
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS_PLAINTEXT"
      in_cluster    = true
    }
  }

  configuration_info {
    arn      = aws_msk_configuration.lakehouse.arn
    revision = aws_msk_configuration.lakehouse.latest_revision
  }

  client_authentication {
    unauthenticated = true
  }

  open_monitoring {
    prometheus {
      jmx_exporter {
        enabled_in_broker = true
      }
      node_exporter {
        enabled_in_broker = true
      }
    }
  }

  logging_info {
    broker_logs {
      s3 {
        enabled = true
        bucket  = aws_s3_bucket.logs.id
        prefix  = "msk-broker-logs/"
      }
    }
  }

  tags = {
    Name        = "${var.project_name}-kafka"
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_ssm_parameter" "schema_registry_url" {
  name        = "/${var.project_name}/${var.environment}/schema-registry/url"
  description = "Confluent Schema Registry URL — MSK does not bundle a schema registry; deploy Confluent SR separately and update this value"
  type        = "SecureString"
  value       = "http://schema-registry.${var.project_name}.internal:8081"

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}
