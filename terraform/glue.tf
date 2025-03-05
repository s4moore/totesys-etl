#database
# (Required) Name of the database. The acceptable characters are lowercase letters, numbers, and the underscore character.
resource "aws_glue_catalog_database" "load_db" {
  name = "load_db"
}

# basic tables
resource "aws_glue_catalog_table" "fact_sales_order" {
  name          = "fact_sales_order"
  database_name = aws_glue_catalog_database.load_db.name
  table_type = "EXTERNAL_TABLE"
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.terrific-totes-processed.bucket}/fact_sales_order"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    }
}

resource "aws_glue_catalog_table" "dim_staff" {
  name          = "dim_staff"
  database_name = aws_glue_catalog_database.load_db.name
  table_type = "EXTERNAL_TABLE"
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.terrific-totes-processed.bucket}/dim_staff"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    }
}

resource "aws_glue_catalog_table" "dim_location" {
  name          = "dim_location"
  database_name = aws_glue_catalog_database.load_db.name
  table_type = "EXTERNAL_TABLE"
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.terrific-totes-processed.bucket}/dim_location"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    }
}

resource "aws_glue_catalog_table" "dim_design" {
  name          = "dim_design"
  database_name = aws_glue_catalog_database.load_db.name
  table_type = "EXTERNAL_TABLE"
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.terrific-totes-processed.bucket}/dim_design"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    }
}

resource "aws_glue_catalog_table" "dim_currency" {
  name          = "dim_currency"
  database_name = aws_glue_catalog_database.load_db.name
  table_type = "EXTERNAL_TABLE"
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.terrific-totes-processed.bucket}/dim_currency"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    }
}

resource "aws_glue_catalog_table" "dim_counterparty"{
  name          = "dim_counterparty"
  database_name = aws_glue_catalog_database.load_db.name
  table_type = "EXTERNAL_TABLE"
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.terrific-totes-processed.bucket}/dim_counterparty"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    }
}

resource "aws_glue_catalog_table" "dim_date"{
  name          = "dim_date"
  database_name = aws_glue_catalog_database.load_db.name
  table_type = "EXTERNAL_TABLE"
  storage_descriptor {
    location      = "s3://${aws_s3_bucket.terrific-totes-processed.bucket}/dim_date"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    }
}

#parquet table for athena - if needed at a later stage

# resource "aws_glue_catalog_table" "aws_glue_catalog_table" {
#   name          = "MyCatalogTable"
#   database_name = "MyCatalogDatabase"

#   table_type = "EXTERNAL_TABLE"

#   parameters = {
#     EXTERNAL              = "TRUE"
#     "parquet.compression" = "SNAPPY"
#   }

#   storage_descriptor {
#     location      = "s3://my-bucket/event-streams/my-stream"
#     input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
#     output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

#     ser_de_info {
#       name                  = "my-stream"
#       serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

#       parameters = {
#         "serialization.format" = 1
#       }
#     }

#     columns {
#       name = "my_string"
#       type = "string"
#     }

#     columns {
#       name = "my_double"
#       type = "double"
#     }

#     columns {
#       name    = "my_date"
#       type    = "date"
#       comment = ""
#     }

#     columns {
#       name    = "my_bigint"
#       type    = "bigint"
#       comment = ""
#     }

#     columns {
#       name    = "my_struct"
#       type    = "struct<my_nested_string:string>"
#       comment = ""
#     }
#   }
# }