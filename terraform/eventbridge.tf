
resource "aws_cloudwatch_event_rule" "every_20_min" {
   name = "every-20-mins"
   description = "Fires every 20 mins"
   schedule_expression = "rate(20 minutes)"
}
resource "aws_cloudwatch_event_target" "check_lambda_every_20_min" {
    rule = aws_cloudwatch_event_rule.every_20_min.name
    target_id = <insert lambda name here>
    arn = aws_lambda_function.<lambda-name-here>.arn
  
}

resource "aws_lambda_permission" "allow_cloudwatch_to_call_lambda_function" {
    statement_id = "AllowExecutionFromCloudWatch"
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.<lambda-name-here>.<function-name-here>
    principal = "events.amazonaws.com"
    source_arn = aws_cloudwatch_event_rule.every_20_min.arn
}
