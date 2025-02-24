resource "aws_s3_bucket" "terrific-totes-data" {
    bucket = "terrific-totes-data-team-11"
    tags = {
        Name = "Terrfic Totes bucket"
        Enviroment = "Dev"
    }
  
}

data "aws_iam_policy_document" "s3_read_only" {
    statement {
      actions = ["s3:GetObject"]
      resources = [ "${aws_s3_bucket.terrific-totes-data.arn}" ]
    }
  
}
resource "aws_iam_policy" "s3_policy" {
    name = "s3-policy"
    policy = data.aws_iam_policy_document.s3_read_only.json
}

