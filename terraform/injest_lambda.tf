module "lambda_function" {
  source = "terraform-aws-modules/lambda/aws"

  function_name = "injest_lambda"
  description   = "Lambda function that injests data from totesys database"
  handler       = "<Insert Lambda Handler Here>" # needs lambda handler here
  runtime       = "python3.12"

  source_path = "${path.module}/../src/<insert path here>" # needs path to src file here

  tags = {
    Name = "injest_lambda"
  }

  layers = [aws_lambda_layer_version.dependencies.arn]

  allowed_triggers = {
    EventBridgeScheduler = {
      principal  = "events.amazonaws.com"
      source_arn = "<insert arn here>" # need to add the arn of the event bridge scheduler
    }
  }

  attach_policy_statements = true
  policy_statements = {
    s3_read_write = {
      effect    = "Allow"
      actions   = ["s3:PutObject", "s3:GetObject"]
      resources = ["${"<insert bucket arn"}/*"] # need to add bucket arn
    },
    cw_full_access = {
      effect    = "Allow"
      actions   = ["logs:*"]         # check with team if they want this narrowed down
      resources = ["arn:aws:logs:*"] # need to narrow this down to just the one log folder for just the injest lambda
    }
  }
}