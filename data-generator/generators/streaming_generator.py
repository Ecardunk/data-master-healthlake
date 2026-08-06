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

        for event_id in self.iter_ids(n_records, starting_id):
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

        return self.build_dataframe(events)
