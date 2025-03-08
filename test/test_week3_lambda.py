from src.week3_lambda import lambda_handler
from unittest.mock import patch, MagicMock
import logging
from pg8000.exceptions import DatabaseError


class TestLoadLambda:

    @patch("src.week3_lambda.db_connection2")
    def test_logs_DataBaseError_for_db_connecion_error(self, mock_con, caplog):
        mock_con.side_effect = DatabaseError("Mock databse error")
        with caplog.at_level(logging.ERROR):
            lambda_handler({}, {})
            assert "Load: DataBaseError:" in caplog.text

    @patch("src.week3_lambda.db_connection2")
    def test_logs_KeyError_if_no_tables_written_data(self, mock_con, caplog):
        mock_con.return_value = MagicMock()
        with caplog.at_level(logging.ERROR):
            lambda_handler({"triggerLambda2": True}, {})
            assert "Load: KeyError:" in caplog.text

    @patch("src.week3_lambda.db_connection2")
    def test_logs_error_if_write_to_db_fails(self, mock_con, caplog):
        mock_con.return_value = MagicMock()
        with caplog.at_level(logging.ERROR):
            lambda_handler({"tables_written": ["dim_lemon"]}, {})
            assert "Load: Exception:" in caplog.text

    @patch("src.week3_lambda.wr.postgresql.to_sql")
    @patch("src.week3_lambda.wr.s3.read_parquet_table")
    @patch("src.week3_lambda.db_connection2")
    def test_to_sql_uses_correct_args_and_data(
        self, mock_con, mock_read_parquet, mock_to_sql, test_df
    ):
        mock_con.return_value = MagicMock()
        mock_read_parquet.return_value = test_df
        mock_to_sql.return_value = MagicMock()

        lambda_handler({"tables_written": ["dim_date"]}, {})
        mock_to_sql.assert_called_once_with(
            df=test_df,
            con=mock_con.return_value,
            schema="project_team_11",
            table="dim_date",
            mode="append",
            index=False,
            insert_conflict_columns=["date_id"],
        )
        mock_con.assert_any_call()
