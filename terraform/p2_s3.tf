resource "aws_s3_bucket" "terrific-totes-processed" {
    bucket = "totes-11-processed-data"
    tags = {
        Name = "Terrific Totes bucket"
    }
    force_destroy = true
}