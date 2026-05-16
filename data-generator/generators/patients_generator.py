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

    def generate(self, n_records: int):

        patients = []

        for patient_id in range(1, n_records + 1):

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