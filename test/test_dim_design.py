from layer2 import dim_design
from testfixtures import LogCapture
import pandas as pd


class TestDimDesign:
    def test_transforms_old_df_to_star_schema_format(self):
        design_columns = [
            "design_id",
            "created_at",
            "design_name",
            "file_location",
            "file_name",
            "last_updated",
        ]
        data = [
            [
                1,
                "2024-11-03 14:20:49.962",
                "Steel",
                "/private",
                "steel-20220717-npgz.json",
                "2024-11-03 14:20:49.962",
            ],
            [
                51,
                "2023-01-12 18:50:09.935",
                "Bronze",
                "/private",
                "bronze-20221024-4dds.json",
                "2023-01-12 18:50:09.935",
            ],
            [
                69,
                "2023-02-07 17:31:10.093",
                "Bronze",
                "/lost+found",
                "bronze-20230102-r904.json",
                "2023-02-07 17:31:10.093",
            ],
        ]
        old_df = pd.DataFrame(data, columns=design_columns)
        output = dim_design(old_df)
        assert list(output.columns) == [
            "design_id",
            "design_name",
            "file_location",
            "file_name",
        ]
        assert list(output.iloc[0]) == [
            1,
            "Steel",
            "/private",
            "steel-20220717-npgz.json",
        ]
        assert list(output.iloc[1]) == [
            51,
            "Bronze",
            "/private",
            "bronze-20221024-4dds.json",
        ]
        assert list(output.iloc[2]) == [
            69,
            "Bronze",
            "/lost+found",
            "bronze-20230102-r904.json",
        ]

    def test_handles_no_df_error(self):
        with LogCapture() as log:
            output = dim_design("")
            assert output == {"result": "Failure"}
            assert "ERROR" in str(log)
            assert "Given paramater should be a DataFrame." in str(log)

        with LogCapture() as log:
            output = dim_design([1, 2, 3])
            assert output == {"result": "Failure"}
            assert "ERROR" in str(log)
            assert "Given paramater should be a DataFrame." in str(log)

        with LogCapture() as log:
            output = dim_design({"a": 1, "b": 2, "c": 3})
            assert output == {"result": "Failure"}
            assert "ERROR" in str(log)
            assert "Given paramater should be a DataFrame." in str(log)
