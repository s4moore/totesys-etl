
# Totes Data Engineering Project - ETL

A project to perform an extract, transform and load process for the sales data of NorthCoders Totes Ltd. 
 
Deploying via Terraform using Infrastructure as code, this project allows for ingesting from AWS RDS, transforming into a star schema and loading into a seperate AWS RDS to allow for business analytics.

![Project Overview](https://i.imgur.com/26cnqux.png)

### Key Components

1. **Job Scheduler - Using AWS EventBridge and Step Function**  
   This code will deploy a job scheduler to manage the ingestion job and subsequent processes. AWS EventBridge, in combination with AWS Step Functions, is used to trigger and orchestrate the processes. The step function is set to run regulary (at 20 minute intervals), ensuring that data changes are checked and available in the data warehouse within 30 minutes of being written to the database.

2. **S3 Bucket - Landing Zone for Ingested Data**  
   An S3 bucket is created to serve as the "landing zone" for ingested data.

3. **Python Application - Ingest/Extract Lambda**  
   This Python application checks for changes in the database and ingests any new or updated data. AWS Lambda is used as the computing solution due to its simplicity in orchestration, monitoring, and deployment.  The ingested data is saved in the "ingestion" S3 bucket in the pickle format, with status and error messages logged to CloudWatch.

4. **CloudWatch Alert for Errors**  
   CloudWatch alerts are set up to trigger an email notification in the event of a major error, ensuring timely awareness and action.

5. **Second S3 Bucket - Processed Data**  
   A second S3 bucket is created to store the processed/transformed data.

6. **Python Application - Transform Lambda**  
   This Python application processes the ingested data from the "ingestion" S3 bucket and transforms it to conform to the warehouse schema. The transformed data is then placed in the "processed" S3 bucket, saved in parquet format.  The transformation is triggered via a "choice" in the step function - if new data is identified as part of the Extraction Lambda.  Status and error messages are logged to CloudWatch, with alerts configured for critical issues.

7. **Python Application - Load Lambda**  
   A Python application is scheduled as part of the step function following the data transformation application to update the data warehouse with data from the "processed" S3 bucket. Status and error logs are sent to CloudWatch, with alerts for major errors to ensure smooth operation.

   The resulting star schema:

![Star Schema Format](https://i.imgur.com/bMsL2CG.png)

## Run Locally

Clone the project

```bash
  git clone https://github.com/svet-g/de-project
```

Go to the project directory

```bash
  cd de-project
```

Install dependencies

```bash
  make requirements
```

## Deployment

To deploy this project ensure that AWS credentials are stored as environment variables locally

Ensure that a bucket exists for your state file to be stored in and amend the provider.tf file

Terraform can that be deployed using the commands

```bash
  terraform init
  terraform apply
```


## Running Tests

Both unit tests and integration tests have been provided as part of the CI/CD pipeline. 

Test coverage of >90%

To run tests, run the following commands

```bash
  make dev-setup
  make run-checks
```

## Authors

- [@Hugues](https://www.github.com/Hugues)
- [@maxMarty](https://www.github.com/maxMarty) 
- [@SSM-F](https://www.github.com/SSM-F)
- [@svet-g](https://www.github.com/svet-g)
- [@s4moore](https://www.github.com/s4moore)
- [@valdemotch](https://www.github.com/valdemotch) 
