terraform {
  required_providers {
    aws ={
        source = "hashicorp/aws"
        version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "state-123321"
    key= "terraform/state.tfstate"
    region = "eu-west-2"
  }
}
provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
        ProjectName = "Terrific Totes"
        Team = "team-11-wicklow"
        DeployedFrom = "Terraform"
        Repository= "de-project"
        Enviroment= "dev"
    }
  }
}
