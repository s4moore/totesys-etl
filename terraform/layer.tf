data "archive_file" "layer" {
  type        = "zip"
  output_path = "${path.module}/../packages/layer/layer.zip"
  source_dir  = "${path.module}/../layer/python"
}

resource "aws_lambda_layer_version" "dependencies" {
  layer_name = "dependencies_layer"
  filename            = data.archive_file.layer.output_path
  source_code_hash    = data.archive_file.layer.output_base64sha256
  compatible_runtimes = ["python3.12", "python3.13"]
}