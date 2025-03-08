from layer2 import load_df_to_s3
import logging


class TestLoadDFToS3:
    def test_puts_object_into_specified_bucket(
        self, mock_aws_with_buckets_and_glue, test_df
    ):
        s3_client, _ = mock_aws_with_buckets_and_glue
        test_bucket = "totes-11-processed-data"
        db_name = "load_db"
        table_name = "test_table"
        load_df_to_s3(test_df, test_bucket, db_name, table_name)
        response = s3_client.list_objects_v2(
            Bucket=test_bucket).get("Contents")
        bucket_files = [file["Key"] for file in response]
        for file in bucket_files:
            assert table_name in file
            assert ".parquet" in file

    def test_overwrittes_duplicates_by_partition(
        self, mock_aws_with_buckets_and_glue, test_df
    ):
        s3_client, _ = mock_aws_with_buckets_and_glue
        test_bucket = "totes-11-processed-data"
        db_name = "load_db"
        table_name = "test_table"
        load_df_to_s3(test_df, test_bucket, db_name, table_name)
        load_df_to_s3(test_df, test_bucket, db_name, table_name)
        response = s3_client.list_objects_v2(
            Bucket=test_bucket).get("Contents")
        bucket_files = [file["Key"] for file in response]
        assert len(bucket_files) == 2

    def test_returns_starting_and_error_logs(self, caplog):
        with caplog.at_level(logging.INFO):

            load_df_to_s3("test", "test", "test", "test")

            assert (
                "Transforming dataframe into parquet and loading to s3 bucket"
                in caplog.text
            )
            assert (
                "Unexpected error whilst loading parquet files to s3"
                ) in caplog.text

    def test_returns_success_log(
            self, mock_aws_with_buckets_and_glue, test_df, caplog
            ):
        with caplog.at_level(logging.INFO):

            test_bucket = "totes-11-processed-data"
            db_name = "load_db"
            table_name = "test_table"

            load_df_to_s3(test_df, test_bucket, db_name, table_name)

            assert "Upload successful" in caplog.text
