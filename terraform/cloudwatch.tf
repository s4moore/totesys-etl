resource "aws_cloudwatch_metric_alarm" "error_raise" {
    alarm_name = "injest_lambda_error"
    comparison_operator = "GreaterThanOrEqualToThreshold"
    evaluation_periods = 1
    metric_name = "Errors"
    namespace = "AWS/Lambda"
    period = 10
    statistic = "Sum"
    threshold = 0

    alarm_description = "No new data to pull"
    actions_enabled = true
    alarm_actions = [ aws_sns_topic.injest_lambda.arn ]

}


resource "aws_cloudwatch_log_metric_filter" "metricFilterResource" {
    name = "ErrorFilter"
    pattern = "ERROR"
    log_group_name = "/aws/lambda/injest_lambda"

    metric_transformation {
      name = "TotesEvent"
      namespace = "Totes/Errors"
      value = "1"
    }
  
}