resource "null_resource" "pip_install" {
  provisioner "local-exec" {
    command = "pip install -r ${path.module}/../requirements.txt -t ${path.module}/../dependencies/python"
  }

  triggers = {
    dependencies = filemd5("${path.module}/../requirements.txt")
  }
}

data "archive_file" "layer" {
  type        = "zip"
  output_path = "${path.module}/../packages/layer/layer.zip"
  source_dir  = "${path.module}/../dependencies"
  depends_on = [null_resource.pip_install]
}

resource "aws_lambda_layer_version" "dependencies" {
  layer_name = "dependencies_layer"
  filename            = data.archive_file.layer.output_path
  source_code_hash    = data.archive_file.layer.output_base64sha256
  compatible_runtimes = ["python3.12", "python3.13"] # check with team if need to add more
}