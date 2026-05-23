import pandas as pd
import random

from generators.base_generator import BaseGenerator


class PatientGenerator(BaseGenerator):

    BLOOD_TYPES = [
        "A+",
        "A-",
        "B+",
        "B-",
        "AB+",
        "AB-",
        "O+",
        "O-"
    ]

    def generate(self, n_records: int, starting_id: int):

        patients = []

        start_id = starting_id + 1
        end_id = starting_id + n_records + 1

        for patient_id in range(
            start_id,
            end_id
        ):


            patients.append({
                "patient_id": patient_id,
                "full_name": self.fake.name(),
                "cpf": self.fake.cpf(),
                "email": self.fake.email(),
                "phone": self.fake.phone_number(),
                "gender": random.choice(["M", "F"]),
                "blood_type": random.choice(
                    self.BLOOD_TYPES
                ),
                "birth_date": self.fake.date_of_birth(
                    minimum_age=0,
                    maximum_age=100
                ),
                "city": self.fake.city(),
                "state": self.fake.estado_sigla(),
                "created_at": self.fake.date_time_between(
                    start_date="-2y",
                    end_date="now"
                )
            })

        df = pd.DataFrame(patients)

        return self.apply_data_quality_issues(df)