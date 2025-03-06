resource "aws_cloudwatch_metric_alarm" "error_raise_2" {
    alarm_name = "transform_lambda_error"
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
    name = "ErrorFilter_2"
    pattern = "ERROR"
    log_group_name = "/aws/lambda/transform_lambda"

    metric_transformation {
      name = "TotesEvent_2"
      namespace = "Totes/Errors"
      value = "1"
    }
    depends_on = [ module.lambda_function_2 ]
}