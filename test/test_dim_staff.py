from layer2 import create_dim_staff
from testfixtures import LogCapture
import pandas as pd


class TestStaff:
    def test_dataframe_for_staff_deparment_merged_dataframe(self):
        staff_columns = [
            "staff_id",
            "first_name",
            "last_name",
            "department_id",
            "email_address",
            "created_at",
            "last_updated",
        ]
        data = [
            [
                "1",
                "Jeremie",
                "Franey",
                "2",
                "jeremie.franey@terrifictotes.com",
                "2022-11-03 14:20:51.563",
                "2022-11-03 14:20:51.563",
            ],
            [
                "2",
                "Deron",
                "Beier",
                "6",
                "deron.beier@terrifictotes.com",
                "2022-11-03 14:20:51.563",
                "2022-11-03 14:20:51.563",
            ],
            [
                "3",
                "Jeanette",
                "Erdman",
                "6",
                "jeanette.erdman@terrifictotes.com",
                "2022-11-03 14:20:51.563",
                "2022-11-03 14:20:51.563",
            ],
        ]

        staff = pd.DataFrame(data, columns=staff_columns)

        dept_columns = [
            "department_id",
            "department_name",
            "location",
            "manager",
            "created_at",
            "last_updated",
        ]
        dept_data = [
            [
                "1",
                "Sales",
                "Manchester",
                "Richard Roma",
                "2022-11-03 14:20:49.962",
                "2022-11-03 14:20:49.962",
            ],
            [
                "2",
                "Purchasing",
                "Manchester",
                "Naomi Lapaglia",
                "2022-11-03 14:20:49.962",
                "2022-11-03 14:20:49.962",
            ],
            [
                "3",
                "Production",
                "Leeds",
                "Chester Ming",
                "2022-11-03 14:20:49.962",
                "2022-11-03 14:20:49.962",
            ],
        ]
        dept = pd.DataFrame(dept_data, columns=dept_columns)
        test = create_dim_staff(staff, dept)
        assert isinstance(test, pd.DataFrame)

    def test_to_check_empty_inputs(self):
        staff_df = pd.DataFrame(
            columns=[
                "staff_id",
                "first_name",
                "last_name",
                "department_id",
                "email_address",
            ]
        )
        dept_df = pd.DataFrame(
            columns=["department_id", "department_name", "location", "manager"]
        )
        test = create_dim_staff(staff_df, dept_df)
        assert test.empty

    def test_handles_no_df_error(self):
        # i/p str
        with LogCapture() as log:
            test = create_dim_staff("", pd.DataFrame())
            assert test is None
            assert "ERROR" in str(log)
            assert "'str' object has no attribute 'merge'" in str(log)

        # i/p list
        with LogCapture() as log:
            test = create_dim_staff([1, 2, 3], pd.DataFrame())
            assert test is None
            assert "ERROR" in str(log)
            assert "'list' object has no attribute 'merge'" in str(log)

        # i/p dict
        with LogCapture() as log:
            test = create_dim_staff({"a": 1, "b": 2}, pd.DataFrame())
            assert test is None
            assert "ERROR" in str(log)
            assert "'dict' object has no attribute 'merge'" in str(log)
