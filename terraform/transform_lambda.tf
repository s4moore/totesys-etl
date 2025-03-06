module "lambda_function_2" {
  source = "terraform-aws-modules/lambda/aws"

  function_name = "transform_lambda"
  description   = "Lambda function that transforms data from extract s3"
  handler       = "week2_lambda.lambda_handler" # needs lambda handler here
  runtime       = "python3.12"
  publish = true
  timeout = 100
  memory_size = 3008

  source_path = "${path.module}/../src/week2_lambda.py" # needs path to src file here

  tags = {
    Name = "transform_lambda"
  }

  layers = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:16",aws_lambda_layer_version.iso_layer.arn, aws_lambda_layer_version.custom_layer_2.arn]

  attach_policy_statements = true

  role_name = "lambda_role_2"

  policy_statements = {
      s3_read_write = {
        effect    = "Allow",
        actions   = ["s3:PutObject", "s3:GetObject"],
        resources = ["${aws_s3_bucket.terrific-totes-processed.arn}/*","${aws_s3_bucket.terrific-totes-data.arn}/*"]
      },
      s3_read = {
        effect    = "Allow",
        actions   = ["s3:List*"],
        resources = ["${aws_s3_bucket.terrific-totes-processed.arn}", "${aws_s3_bucket.terrific-totes-data.arn}"]
      },
      deny_delete_s3 = {
        effect = "Allow",
        actions = ["s3:Delete*"],
        resources = ["${aws_s3_bucket.terrific-totes-processed.arn}/*"]
      },
      cw_full_access = {
        effect    = "Allow",
        actions   = ["logs:*"],
        resources = ["arn:aws:logs:*"],
      },
      glue_full_access = {
        effect    = "Allow",
        actions   = ["glue:*"],
        resources = ["${aws_glue_catalog_database.load_db.arn}/*", "${aws_glue_catalog_database.load_db.arn}", "arn:aws:glue:eu-west-2:442426868881:*"]
      }
    }
}