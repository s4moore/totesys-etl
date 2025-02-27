resource "aws_s3_bucket" "terrific-totes-data" {
    bucket = "terrific-totes-data-team-11-1"
    tags = {
        Name = "Terrific Totes bucket"
    }  
}

resource "aws_iam_policy" "lambda_s3_policy" {
  name        = "lambda_s3_access_policy"
  description = "Policy for Lambda to access S3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = "arn:aws:s3:::terrific-totes-data-team-11-1"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = "arn:aws:s3:::terrific-totes-data-team-11/*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
  role       = module.lambda_function.lambda_role_name
  policy_arn = aws_iam_policy.lambda_s3_policy.arn
}
