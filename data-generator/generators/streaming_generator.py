import pandas as pd
import random

from generators.base_generator import BaseGenerator


class StreamingEventGenerator(BaseGenerator):

    def generate(
        self,
        n_records: int,
        n_patients: int,
        starting_id: int
    ):

        events = []

        start_id = starting_id + 1
        end_id = starting_id + n_records + 1

        for event_id in range(start_id, end_id):

            events.append({
                "event_id": event_id,
                "patient_id": random.randint(
                    1,
                    n_patients
                ),
                "heart_rate": random.randint(40, 180),
                "oxygen_level": random.randint(70, 100),
                "temperature": round(
                    random.uniform(35, 41),
                    1
                ),
                "blood_pressure_systolic": random.randint(
                    90,
                    180
                ),
                "blood_pressure_diastolic": random.randint(
                    60,
                    120
                ),
                "event_timestamp": self.fake.date_time_between(
                    start_date="-30d",
                    end_date="now"
                )
            })

        df = pd.DataFrame(events)

        return self.apply_data_quality_issues(df)
