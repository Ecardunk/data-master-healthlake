import random

from generators.base_generator import BaseGenerator


class DoctorGenerator(BaseGenerator):

    SPECIALTIES = [
        "Cardiology",
        "Neurology",
        "Orthopedics",
        "Pediatrics",
        "Emergency",
        "Pulmonology"
    ]

    def generate(
        self,
        n_records: int,
        n_hospitals: int,
        starting_id: int
    ):
        doctors = []

        for doctor_id in self.iter_ids(n_records, starting_id):
            doctors.append({
                "doctor_id": doctor_id,
                "doctor_name": self.fake.name(),
                "crm": random.randint(10000, 99999),
                "specialty": random.choice(
                    self.SPECIALTIES
                ),
                "hospital_id": random.randint(
                    1,
                    n_hospitals
                ),
                "created_at": self.fake.date_time_between(
                    start_date="-2y",
                    end_date="now"
                )
            })

        return self.build_dataframe(doctors)
