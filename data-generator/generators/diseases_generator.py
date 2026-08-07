import random

from generators.base_generator import BaseGenerator


class DiseaseGenerator(BaseGenerator):

    CATEGORIES = [
        "Respiratory",
        "Cardiovascular",
        "Neurological",
        "Infectious",
        "Orthopedic"
    ]

    DISEASES = [
        "COVID-19",
        "Influenza",
        "Asthma",
        "Hypertension",
        "Stroke",
        "Fracture",
        "Migraine"
    ]

    def generate(self, n_records: int, starting_id: int):
        diseases = []

        for disease_id in self.iter_ids(n_records, starting_id):
            diseases.append({
                "disease_id": disease_id,
                "disease_name": random.choice(
                    self.DISEASES
                ),
                "category": random.choice(
                    self.CATEGORIES
                ),
                "severity_level": random.randint(1, 5),
                "created_at": self.fake.date_time_between(
                    start_date="-2y",
                    end_date="now"
                )
            })

        return self.build_dataframe(diseases)
