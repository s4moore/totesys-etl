variable "EMAIL" {
  type = string
}

# ingest lambda sns
resource "aws_sns_topic" "ingest_lambda" {
  name = "ingest_lambda_topic"
}

resource "aws_sns_topic_subscription" "ingest_lambda_sub" {
  topic_arn = aws_sns_topic.ingest_lambda.arn
  protocol  = "email"
  endpoint  = var.EMAIL
}