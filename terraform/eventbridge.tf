
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
    role_arn  = aws_iam_role.cloudwatch_role.arn
}

# resource "aws_lambda_permission" "allow_cloudwatch_to_call_lambda_function" {
#     statement_id = "AllowExecutionFromCloudWatch"
#     action = "lambda:InvokeFunction"
#     function_name = module.lambda_function.lambda_function_name
#     principal = "events.amazonaws.com"
#     source_arn = aws_cloudwatch_event_rule.every_20_min.arn
# }

resource "aws_iam_policy" "totes_step_function_policy" {
  name        = "totes_step_func_policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "states:StartExecution"
        Resource = aws_sfn_state_machine.step_function_totes.arn
      }
    ]
  })
}

resource "aws_iam_role" "cloudwatch_role" {
  name               = "cloudwatch_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "sts:AssumeRole"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy_attachment" "totes_step_function_policy_attachment" {
  name       = "totes_step_function_policy_chattachment"
  policy_arn = aws_iam_policy.totes_step_function_policy.arn
  roles      = [aws_iam_role.cloudwatch_role.name]
}