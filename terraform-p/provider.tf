terraform {
  required_providers {
    aws ={
        source = "hashicorp/aws"
        version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "BUCKET-NAME"
    key= 
    
  }
}
provider "aws" {
  region = "eu-west-2"
}
