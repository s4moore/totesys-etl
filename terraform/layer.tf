data "archive_file" "layer" {
  type        = "zip"
  output_path = "${path.module}/../packages/layer/layer.zip"
  source_dir  = "${path.module}/../layer"
}

resource "aws_lambda_layer_version" "dependencies" {
  layer_name = "dependencies_layer"
  filename            = data.archive_file.layer.output_path
  source_code_hash    = data.archive_file.layer.output_base64sha256
  compatible_runtimes = ["python3.12", "python3.13"]
}

data "archive_file" "custom_layer" {
  type        = "zip"
  output_path = "${path.module}/../packages/layer/custom_layer.zip"
  source_dir  = "${path.module}/../custom_layer"
}

resource "aws_lambda_layer_version" "custom_layer" {
  layer_name = "custom_layer"
  filename            = data.archive_file.custom_layer.output_path
  source_code_hash    = data.archive_file.custom_layer.output_base64sha256
  compatible_runtimes = ["python3.12", "python3.13"]
}

