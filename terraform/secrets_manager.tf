data "aws_caller_identity" "current" {}

variable "cohort_id" {}
variable "user" {}
variable "password" {}
variable "host" {}
variable "database" {}
variable "port" {}

module "secrets_manager" {
  source = "terraform-aws-modules/secrets-manager/aws"

  # Secret
  name        = "totesys-testing-8"
  description = "Totesys database credentials"
  secret_string = jsonencode({ "cohort_id" : "${var.cohort_id}",
    "user" : "${var.user}",
    "password" : "${var.password}",
    "host" : "${var.host}",
    "database" : "${var.database}",
  "port" : var.port })

  # Policy
  create_policy = true
  policy_statements = {
    read = {
      sid = "AllowAccountRead"
      principals = [{
        type        = "AWS"
        identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
      }]
      actions   = ["secretsmanager:GetSecretValue"]
      resources = ["*"]
    }
  }

  tags = {
    Environment = "Development"
    Project     = "Terrific Totes"
  }
}

resource "aws_secretsmanager_secret" "example" {
  name = "example"
}

data "aws_iam_policy_document" "example" {
  statement {
    sid    = "AllowAccountRead"
    effect = "Allow"

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
    }

    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["*"]
  }
}

resource "aws_secretsmanager_secret_policy" "example" {
  secret_arn = aws_secretsmanager_secret.example.arn
  policy     = data.aws_iam_policy_document.example.json
}