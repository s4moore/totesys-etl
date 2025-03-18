resource "aws_s3_bucket" "terrific-totes-data" {
    bucket = "ingest123"
    tags = {
        Name = "Terrific Totes bucket"
    }
    force_destroy = true
}