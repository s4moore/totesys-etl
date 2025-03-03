resource "aws_s3_bucket" "terrific-totes-data" {
    bucket = "totes-11-processed-data"
    tags = {
        Name = "Terrific Totes bucket"
    }  
}