resource "aws_cloudwatch_log_group" "totes404TerraformGroup" {
    name = "/aws/lambda/${aws_lambda_function.<lambda-name-handler>.function_name}"
  
}
resource "aws_cloudwatch_metric_alarm" "error_raise" {
    alarm_name = "totes404DataError"
    comparison_operator = "GreaterThanOrEqualToThreshold"
    evaluation_periods = 1
    metric_name = "Errors"
    namespace = "AWS/Lambda"
    period = 10
    statistic = "Sum"
    threshold = 0

    alarm_description = "No new data to pull"
    actions_enabled = true
    alarm_actions = [ aws_sns_topic.<sns-name>.arn ]

}

resource "aws_cloudwatch_log_metric_filter" "metricFilterResource" {
    name = "ErrorFilter"
    pattern = "{($.eventName= ErrorInDataFlow) && ($.errorMessage = \"Something happened while trying to pull new data\")}"
    log_group_name = aws_cloudwatch_log_group.totes404TerraformGroup.name

    metric_transformation {
      name = "TotesEvent"
      namespace = "Totes/Errors"
      value = "1"
    }
  
}

