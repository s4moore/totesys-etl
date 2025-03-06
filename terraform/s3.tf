resource "aws_s3_bucket" "terrific-totes-data" {
    bucket = "terrific-totes-data-team-11"
    tags = {
        Name = "Terrific Totes bucket"
    }
    force_destroy = true
}