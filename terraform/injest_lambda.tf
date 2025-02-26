module "lambda_function" {
  source = "terraform-aws-modules/lambda/aws"

  function_name = "injest_lambda"
  description   = "Lambda function that injests data from totesys database"
  handler       = "dummy.dummy" # needs lambda handler here
  runtime       = "python3.12"
  publish = true

  source_path = "${path.module}/../src/dummy.py" # needs path to src file here

  tags = {
    Name = "injest_lambda"
  }

  layers = [aws_lambda_layer_version.dependencies.arn]

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
    }
  }
}