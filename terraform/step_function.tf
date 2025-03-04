# ...

resource "aws_sfn_state_machine" "step_function_totes" {
  name     = "step_function_totes"
  role_arn = aws_iam_role.iam_for_step_machine.arn

  definition = <<EOF

{
  "QueryLanguage": "JSONPath",
  "Comment": "A description of my state machine",
  "StartAt": "ingest Lambda Invoke",
  "States": {
    "ingest Lambda Invoke": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "Payload.$": "$",
        "FunctionName": "arn:aws:lambda:eu-west-2:442426868881:function:injest_lambda:$LATEST"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL"
        }
      ],
      "Next": "Choice"
    },
    "Choice": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.triggerLambda2",
          "BooleanEquals": false,
          "Next": "Pass"
        }
      ],
      "Default": "Transform Lambda Invoke"
    },
    "Pass": {
      "Type": "Pass",
      "End": true
    },
    "Transform Lambda Invoke": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "Payload.$": "$",
        "FunctionName": "arn:aws:lambda:eu-west-2:442426868881:function:transform_lambda:$LATEST"
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException"
          ],
          "IntervalSeconds": 1,
          "MaxAttempts": 3,
          "BackoffRate": 2,
          "JitterStrategy": "FULL"
        }
      ],
      "End": true
    }
  }
}

EOF
}

resource "aws_iam_role" "iam_for_step_machine" {
  name = "step_machine_role"

  # Terraform's "jsonencode" function converts a
  # Terraform expression result to valid JSON syntax.
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid    = ""
        Principal = {
          Service = "states.amazonaws.com"
        }
      },
    ]
  })

  tags = {
    tag-key = "tag-value"
  }
}
resource "aws_iam_policy" "step_func_policy" {
  name        = "step_func_policy"
  path        = "/"
  description = "step_funcpolicy"

  # Terraform's "jsonencode" function converts a
  # Terraform expression result to valid JSON syntax.
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
        {
            "Effect": "Allow",
            "Action": [
                "lambda:InvokeFunction"
            ],
            "Resource": [
                "arn:aws:lambda:eu-west-2:442426868881:function:injest_lambda:*",
                "arn:aws:lambda:eu-west-2:442426868881:function:transform_lambda:*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "lambda:InvokeFunction"
            ],
            "Resource": [
                "arn:aws:lambda:eu-west-2:442426868881:function:injest_lambda",
                "arn:aws:lambda:eu-west-2:442426868881:function:transform_lambda"
            ]
        }
    ]
  })
}
resource "aws_iam_policy_attachment" "attach_step_role" {
  name       = "step_func_attached"
  roles      = [aws_iam_role.iam_for_step_machine.name]
  policy_arn = aws_iam_policy.step_func_policy.arn
}
