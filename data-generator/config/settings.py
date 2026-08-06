from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
OUTPUT_DIR_RAW = BASE_DIR / "output" / "raw"
OUTPUT_DIR_STREAMING = BASE_DIR / "output" / "streaming"
METADATA_FILE = BASE_DIR / "metadata" / "id_control.json"

RECORD_COUNTS = {
    "patients": 5000,
    "hospitals": 5,
    "doctors": 12,
    "diseases": 2,
    "attendance": 1000
}

DATASET_PROFILES = {
    "hospitals": {
        "churn_percentage": 0.005,
        "duplicate_percentage": 0.001,
        "null_percentages": {
            "hospital_type": 0.005,
            "city": 0.010,
            "state": 0.005,
            "capacity": 0.003
        }
    },
    "patients": {
        "churn_percentage": 0.010,
        "duplicate_percentage": 0.002,
        "null_percentages": {
            "email": 0.080,
            "phone": 0.060,
            "gender": 0.002,
            "blood_type": 0.030,
            "birth_date": 0.004,
            "city": 0.020,
            "state": 0.010
        }
    },
    "doctors": {
        "churn_percentage": 0.020,
        "duplicate_percentage": 0.001,
        "null_percentages": {
            "crm": 0.003,
            "specialty": 0.010,
            "hospital_id": 0.002
        }
    },
    "diseases": {
        "churn_percentage": 0.000,
        "duplicate_percentage": 0.000,
        "null_percentages": {
            "category": 0.005,
            "severity_level": 0.002
        }
    },
    "attendance": {
        "churn_percentage": 0.001,
        "duplicate_percentage": 0.0015,
        "null_percentages": {
            "doctor_id": 0.001,
            "hospital_id": 0.001,
            "wait_time_minutes": 0.025,
            "cost": 0.015,
            "severity_score": 0.003,
            "discharge_flag": 0.010
        }
    }
}
