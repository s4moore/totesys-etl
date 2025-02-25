variable "email" {}

# injest lambda sns
resource "aws_sns_topic" "injest_lambda" {
  name = "injest_lambda-topic"
}

resource "aws_sns_topic_subscription" "injest_lambda-sub" {
  topic_arn = aws_sns_topic.injest_lambda.arn
  protocol  = "email"
  endpoint  = "${var.email}"
}