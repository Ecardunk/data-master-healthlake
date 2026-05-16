from faker import Faker
from utils.dirty_data_utils import (
    inject_duplicates,
    inject_nulls
)


class BaseGenerator:

    def __init__(
        self,
        null_percentage=0.05,
        duplicate_percentage=0.03
    ):

        self.fake = Faker("pt_BR")

        self.null_percentage = null_percentage
        self.duplicate_percentage = duplicate_percentage

    def apply_data_quality_issues(self, df):

        df = inject_nulls(
            df,
            self.null_percentage
        )

        df = inject_duplicates(
            df,
            self.duplicate_percentage
        )

        return df