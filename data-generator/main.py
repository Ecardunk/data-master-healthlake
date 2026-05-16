from config.settings import *

from utils.file_utils import ensure_directories

from generators.patients_generator import PatientGenerator
from generators.hospitals_generator import HospitalGenerator
from generators.doctors_generator import DoctorGenerator
from generators.diseases_generator import DiseaseGenerator
from generators.attendance_generator import AttendanceGenerator
# from generators.streaming_generator import (
#     StreamingEventGenerator
# )

# =========================================================
# CREATE DIRECTORIES
# =========================================================

ensure_directories([
    OUTPUT_DIR_RAW
])

# =========================================================
# GENERATORS
# =========================================================

print("Generating hospitals...")
hospital_df = HospitalGenerator().generate(
    N_HOSPITALS
)

print("Generating patients...")
patient_df = PatientGenerator().generate(
    N_PATIENTS
)

print("Generating doctors...")
doctor_df = DoctorGenerator().generate(
    N_DOCTORS,
    N_HOSPITALS
)

print("Generating diseases...")
disease_df = DiseaseGenerator().generate(
    N_DISEASES
)

print("Generating attendance...")
attendance_df = AttendanceGenerator().generate(
    N_ATTENDANCE,
    N_PATIENTS,
    N_DOCTORS,
    N_HOSPITALS,
    N_DISEASES
)

# print("Generating streaming events...")
# streaming_df = StreamingEventGenerator().generate(
#     N_STREAMING_EVENTS,
#     N_PATIENTS
# )

# =========================================================
# SAVE FILES
# =========================================================

hospital_df.to_csv(
    f"{OUTPUT_DIR_RAW}/hospitals.csv",
    index=False
)

patient_df.to_csv(
    f"{OUTPUT_DIR_RAW}/patients.csv",
    index=False
)

doctor_df.to_csv(
    f"{OUTPUT_DIR_RAW}/doctors.csv",
    index=False
)

disease_df.to_csv(
    f"{OUTPUT_DIR_RAW}/diseases.csv",
    index=False
)

attendance_df.to_csv(
    f"{OUTPUT_DIR_RAW}/attendance.csv",
    index=False
)

# streaming_df.to_json(
#     f"{OUTPUT_DIR_STREAMING}/streaming_events.json",
#     orient="records",
#     lines=True
# )

print("\n===================================")
print("All datasets successfully generated")
print("===================================")