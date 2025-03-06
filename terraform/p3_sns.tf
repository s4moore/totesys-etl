# load lambda sns
resource "aws_sns_topic" "load_lambda" {
  name = "load_lambda_topic"
}

resource "aws_sns_topic_subscription" "load_lambda_sub" {
  topic_arn = aws_sns_topic.load_lambda.arn
  protocol  = "email"
  endpoint  = var.EMAIL
}