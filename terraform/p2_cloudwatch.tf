resource "aws_cloudwatch_log_group" "totes404TerraformGroup_2" {
    name = "/aws/lambda/${module.lambda_function_2.lambda_function_name}-phase-2"
  
}
resource "aws_cloudwatch_metric_alarm" "error_raise_2" {
    alarm_name = "totes404DataError"
    comparison_operator = "GreaterThanOrEqualToThreshold"
    evaluation_periods = 1
    metric_name = "Errors"
    namespace = "AWS/Lambda"
    period = 10
    statistic = "Sum"
    threshold = 0

    alarm_description = "No new data to transform"
    actions_enabled = true
    alarm_actions = [ aws_sns_topic.transform_lambda.arn ]

}


resource "aws_cloudwatch_log_metric_filter" "metricFilterResource_2" {
    name = "ErrorFilter"
    pattern = "ERROR"
    log_group_name = aws_cloudwatch_log_group.totes404TerraformGroup_2.name

    metric_transformation {
      name = "TotesEvent"
      namespace = "Totes/Errors"
      value = "1"
    }
  
}