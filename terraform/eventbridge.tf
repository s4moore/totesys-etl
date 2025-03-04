
resource "aws_cloudwatch_event_rule" "every_20_min" {
   name = "every-20-mins"
   description = "Fires every 20 mins"
   schedule_expression = "rate(20 minutes)"
}
resource "aws_cloudwatch_event_target" "check_lambda_every_20_min" {
    rule = aws_cloudwatch_event_rule.every_20_min.name
    target_id = aws_sfn_state_machine.step_function_totes.name
    arn = aws_sfn_state_machine.step_function_totes.arn
    # target_id = module.lambda_function.lambda_function_name
    # arn = module.lambda_function.lambda_function_arn
}

resource "aws_lambda_permission" "allow_cloudwatch_to_call_lambda_function" {
    statement_id = "AllowExecutionFromCloudWatch"
    # action = "lambda:InvokeFunction"
    action = "states:StartExecution"
    # function_name = module.lambda_function.lambda_function_name
    function_name = aws_sfn_state_machine.step_function_totes.name
    principal = "events.amazonaws.com"
    source_arn = aws_cloudwatch_event_rule.every_20_min.arn
}
