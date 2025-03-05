data "archive_file" "custom_layer_2" {
  type        = "zip"
  output_path = "${path.module}/../packages/layer/custom_layer_2.zip"
  source_dir  = "${path.module}/../custom_layer_2"
}

resource "aws_lambda_layer_version" "custom_layer_2" {
  layer_name = "custom_layer_2"
  filename            = data.archive_file.custom_layer_2.output_path
  source_code_hash    = data.archive_file.custom_layer_2.output_base64sha256
  compatible_runtimes = ["python3.12", "python3.13"]
}

data "archive_file" "iso_layer" {
  type        = "zip"
  output_path = "${path.module}/../packages/layer/iso_layer.zip"
  source_dir  = "${path.module}/../isolayer"
}

resource "aws_lambda_layer_version" "iso_layer" {
  layer_name = "iso_layer"
  filename            = data.archive_file.iso_layer.output_path
  source_code_hash    = data.archive_file.iso_layer.output_base64sha256
  compatible_runtimes = ["python3.12", "python3.13"]
}

