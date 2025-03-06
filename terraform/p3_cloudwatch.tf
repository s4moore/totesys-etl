resource "aws_cloudwatch_metric_alarm" "error_raise_3" {
    alarm_name = "load_lambda_error"
    comparison_operator = "GreaterThanOrEqualToThreshold"
    evaluation_periods = 1
    metric_name = "Errors"
    namespace = "AWS/Lambda"
    period = 10
    statistic = "Sum"
    threshold = 0

    alarm_description = "Error while loading files to data warehouse"
    actions_enabled = true
    alarm_actions = [ aws_sns_topic.load_lambda.arn ]

}


resource "aws_cloudwatch_log_metric_filter" "metricFilterResource_3" {
    name = "ErrorFilter_3"
    pattern = "ERROR"
    log_group_name = "/aws/lambda/load_lambda"

    metric_transformation {
      name = "TotesEvent_2"
      namespace = "Totes/Errors"
      value = "1"
    }
    depends_on = [ module.lambda_function_3 ]
}