resource "aws_s3_bucket" "terrific-totes-processed" {
  bucket = "processed123321"
  tags = {
    Name = "Terrific Totes bucket"
  }
  force_destroy = true
}