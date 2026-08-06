import pandas as pd
from faker import Faker

from utils.dirty_data_utils import (
    inject_duplicates,
    inject_nulls
)


class BaseGenerator:
    def __init__(
        self,
        null_percentages=None,
        duplicate_percentage=0.00
    ):
        self.fake = Faker("pt_BR")
        self.null_percentages = null_percentages or {}
        self.duplicate_percentage = duplicate_percentage

    def apply_data_quality_issues(self, df):
        df = inject_nulls(
            df,
            self.null_percentages,
            excluded_columns=["created_at"]
        )

        df = inject_duplicates(
            df,
            self.duplicate_percentage
        )

        return df

    @staticmethod
    def iter_ids(n_records: int, starting_id: int):
        if n_records <= 0:
            raise ValueError("n_records must be greater than zero")

        return range(
            starting_id + 1,
            starting_id + n_records + 1
        )

    def build_dataframe(self, records):
        return self.apply_data_quality_issues(
            pd.DataFrame(records)
        )
