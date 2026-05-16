import pandas as pd
import random
import numpy as np

from generators.base_generator import BaseGenerator


class AttendanceGenerator(BaseGenerator):

    def generate(
        self,
        n_records: int,
        n_patients: int,
        n_doctors: int,
        n_hospitals: int,
        n_diseases: int
    ):

        attendance = []

        for attendance_id in range(1, n_records + 1):

            severity = np.random.choice(
                [1, 2, 3, 4, 5],
                p=[0.40, 0.30, 0.15, 0.10, 0.05]
            )

            attendance_date = self.fake.date_time_between(
                start_date="-2y",
                end_date="now"
            )

            attendance.append({
                "attendance_id": attendance_id,
                "patient_id": random.randint(
                    1,
                    n_patients
                ),
                "doctor_id": random.randint(
                    1,
                    n_doctors
                ),
                "hospital_id": random.randint(
                    1,
                    n_hospitals
                ),
                "disease_id": random.randint(
                    1,
                    n_diseases
                ),
                "attendance_date": attendance_date,
                "wait_time_minutes": random.randint(5, 300),
                "cost": round(
                    random.uniform(100, 20000),
                    2
                ),
                "severity_score": severity,
                "discharge_flag": random.choice([0, 1]),
                "created_at": attendance_date
            })

        df = pd.DataFrame(attendance)

        return self.apply_data_quality_issues(df)