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

    def generate(self, n_records: int, starting_id: int):
        hospitals = []

        for hospital_id in self.iter_ids(n_records, starting_id):
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

        return self.build_dataframe(hospitals)
