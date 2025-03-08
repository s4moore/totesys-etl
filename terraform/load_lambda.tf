module "lambda_function_3" {
  source = "terraform-aws-modules/lambda/aws"

  function_name = "load_lambda"
  description   = "Lambda function that transforms data from extract s3"
  handler       = "week3_lambda.lambda_handler" # needs lambda handler here
  runtime       = "python3.12"
  publish = true
  timeout = 100
  memory_size = 3008

  source_path = "${path.module}/../src/week3_lambda.py" # needs path to src file here

  tags = {
    Name = "transform_lambda"
  }

  layers = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:16", aws_lambda_layer_version.dependencies.arn, aws_lambda_layer_version.custom_layer.arn]

  attach_policy_statements = true

  role_name = "lambda_role_3"

  policy_statements = {
      s3_read_write = {
        effect    = "Allow",
        actions   = ["s3:GetObject"],
        resources = ["${aws_s3_bucket.terrific-totes-processed.arn}/*"]
      },
      s3_read = {
        effect    = "Allow",
        actions   = ["s3:List*"],
        resources = ["${aws_s3_bucket.terrific-totes-processed.arn}"]
      },
      cw_full_access = {
        effect    = "Allow",
        actions   = ["logs:*"],
        resources = ["arn:aws:logs:*"],
      },
      glue_full_access = {
        effect    = "Allow",
        actions   = ["glue:*"],
        resources = ["arn:aws:glue:eu-west-2:442426868881:*"]
      },
        read_secrets = {
        effect    = "Allow",
        actions   = ["secretsmanager:GetSecretValue"],
        resources = ["*"]
      }
    }
}