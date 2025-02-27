module "lambda_function" {
  source = "terraform-aws-modules/lambda/aws"

  function_name = "injest_lambda"
  description   = "Lambda function that injests data from totesys database"
  handler       = "week1_lambda.lambda_handler" # needs lambda handler here
  runtime       = "python3.12"
  publish = true
  timeout = 100

  source_path = "${path.module}/../src/week1_lambda.py" # needs path to src file here

  tags = {
    Name = "injest_lambda"
  }

  layers = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:16",aws_lambda_layer_version.dependencies.arn, aws_lambda_layer_version.custom_layer.arn]

  allowed_triggers = {
    EventBridgeScheduler = {
      principal  = "events.amazonaws.com"
      source_arn = aws_cloudwatch_event_rule.every_20_min.arn
    }
  }

  attach_policy_statements = true
  policy_statements = {
    s3_read_write = {
      effect    = "Allow"
      actions   = ["s3:PutObject", "s3:GetObject", "s3:ListBucket"]
      resources = ["${aws_s3_bucket.terrific-totes-data.arn}/*"]
    },
    deny_delete_s3 = {
      effect = "Deny"
      actions = ["s3:Delete*"]
      resources = ["${aws_s3_bucket.terrific-totes-data.arn}/*"]
    },
    cw_full_access = {
      effect    = "Allow"
      actions   = ["logs:*"] 
      resources = ["arn:aws:logs:*"] # need to narrow this down to just the one log folder for just the injest lambda
    },
      read_secrets = {
      effect    = "Allow",
      actions   = ["secretsmanager:GetSecretValue"],
      resources = ["*"]
    }
  }
}