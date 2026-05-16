import pandas as pd
import random

from generators.base_generator import BaseGenerator


class HospitalGenerator(BaseGenerator):

    HOSPITAL_TYPES = [
        "Public",
        "Private",
        "Emergency",
        "University",
        "Specialized"
    ]

    def generate(self, n_records: int):

        hospitals = []

        for hospital_id in range(1, n_records + 1):

            hospitals.append({
                "hospital_id": hospital_id,
                "hospital_name": f"Hospital {self.fake.company()}",
                "hospital_type": random.choice(
                    self.HOSPITAL_TYPES
                ),
                "state": self.fake.estado_sigla(),
                "city": self.fake.city(),
                "capacity": random.randint(50, 1000),
                "created_at": self.fake.date_time_between(
                    start_date="-2y",
                    end_date="now"
                )
            })

        df = pd.DataFrame(hospitals)

        return self.apply_data_quality_issues(df)