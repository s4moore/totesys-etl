# transform lambda sns
resource "aws_sns_topic" "transform_lambda" {
  name = "transform_lambda_topic"
}

resource "aws_sns_topic_subscription" "transform_lambda_sub" {
  topic_arn = aws_sns_topic.transform_lambda.arn
  protocol  = "email"
  endpoint  = var.EMAIL
}