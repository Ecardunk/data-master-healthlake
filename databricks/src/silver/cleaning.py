"""Pure Bronze-to-Silver cleaning transformations shared with the DQ gate."""

from pyspark.sql import Window
from pyspark.sql import functions as F


TABLE_KEYS = {
    "patients": "patient_id",
    "hospitals": "hospital_id",
    "doctors": "doctor_id",
    "diseases": "disease_id",
    "attendance": "attendance_id",
}
ODATE_PATH_PATTERN = r"(?:^|/)odate=(\d{4}-\d{2}-\d{2})(?:/|$)"
CLEANUP_DROP_NULLS = {
    "patients": ["birth_date", "gender", "state"],
    "hospitals": ["capacity", "state"],
    "doctors": ["crm", "hospital_id"],
    "diseases": ["severity_level"],
    "attendance": [
        "patient_id",
        "doctor_id",
        "hospital_id",
        "disease_id",
        "attendance_timestamp",
        "severity_score",
    ],
}


def try_cast(column_name: str, data_type: str):
    """Cast source text without turning malformed raw values into Spark errors."""
    return F.expr(f"try_cast(`{column_name}` AS {data_type})")


def try_integral(column_name: str, data_type: str):
    """Accept integer CSV values serialized with a decimal suffix, such as 17.0."""
    return F.expr(
        f"try_cast(try_cast(`{column_name}` AS DOUBLE) AS {data_type})"
    )


def with_effective_odate(dataframe):
    """Recover a missing odate from the immutable source path in memory."""
    path_odate = F.to_date(
        F.regexp_extract(F.col("_source_file"), ODATE_PATH_PATTERN, 1),
        "yyyy-MM-dd",
    )
    return dataframe.withColumn("odate", F.coalesce(F.col("odate"), path_odate))


def mask_patient_pii():
    """Return irreversible presentation masks for direct patient identifiers."""
    cpf_digits = F.regexp_replace(F.col("cpf"), r"\D", "")
    phone_digits = F.regexp_replace(F.col("phone"), r"\D", "")

    return {
        "full_name": F.when(
            F.col("full_name").isNull(), F.lit(None).cast("string")
        ).otherwise(F.concat(F.substring(F.trim("full_name"), 1, 1), F.lit("***"))),
        "cpf": F.when(F.col("cpf").isNull(), F.lit(None).cast("string")).otherwise(
            F.concat(F.lit("***.***.***-"), F.substring(cpf_digits, -2, 2))
        ),
        "email": F.when(F.col("email").isNull(), F.lit(None).cast("string")).otherwise(
            F.concat(
                F.substring(F.lower(F.trim("email")), 1, 1),
                F.lit("***@"),
                F.regexp_extract(F.lower(F.trim("email")), r"@(.+)$", 1),
            )
        ),
        "phone": F.when(F.col("phone").isNull(), F.lit(None).cast("string")).otherwise(
            F.concat(F.lit("***-"), F.substring(phone_digits, -4, 4))
        ),
    }


def deduplicate_snapshot(dataframe, key_column: str):
    """Keep the latest copy of each key inside an already-filtered snapshot."""
    latest_record_window = Window.partitionBy(key_column).orderBy(
        F.col("_ingested_at").desc_nulls_last(),
        F.col("_source_file").desc_nulls_last(),
    )

    return (
        dataframe.withColumn("_row_number", F.row_number().over(latest_record_window))
        .where(F.col("_row_number") == 1)
        .drop("_row_number", "_rescued_data", "_corrupt_record")
    )


def clean_patients(dataframe):
    masks = mask_patient_pii()
    return dataframe.select(
        try_integral("patient_id", "BIGINT").alias("patient_id"),
        masks["full_name"].alias("full_name"),
        masks["cpf"].alias("cpf"),
        masks["email"].alias("email"),
        masks["phone"].alias("phone"),
        F.upper(F.trim("gender")).alias("gender"),
        F.upper(F.trim("blood_type")).alias("blood_type"),
        try_cast("birth_date", "DATE").alias("birth_date"),
        F.trim("city").alias("city"),
        F.upper(F.trim("state")).alias("state"),
        try_cast("created_at", "TIMESTAMP").alias("created_at"),
        F.col("odate"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )


def clean_hospitals(dataframe):
    return dataframe.select(
        try_integral("hospital_id", "BIGINT").alias("hospital_id"),
        F.trim("hospital_name").alias("hospital_name"),
        F.trim("hospital_type").alias("hospital_type"),
        F.upper(F.trim("state")).alias("state"),
        F.trim("city").alias("city"),
        try_integral("capacity", "INT").alias("capacity"),
        try_cast("created_at", "TIMESTAMP").alias("created_at"),
        F.col("odate"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )


def clean_doctors(dataframe):
    return dataframe.select(
        try_integral("doctor_id", "BIGINT").alias("doctor_id"),
        F.trim("doctor_name").alias("doctor_name"),
        try_integral("crm", "BIGINT").alias("crm"),
        F.trim("specialty").alias("specialty"),
        try_integral("hospital_id", "BIGINT").alias("hospital_id"),
        try_cast("created_at", "TIMESTAMP").alias("created_at"),
        F.col("odate"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )


def clean_diseases(dataframe):
    return dataframe.select(
        try_integral("disease_id", "BIGINT").alias("disease_id"),
        F.trim("disease_name").alias("disease_name"),
        F.trim("category").alias("category"),
        try_integral("severity_level", "INT").alias("severity_level"),
        try_cast("created_at", "TIMESTAMP").alias("created_at"),
        F.col("odate"),
        F.col("_source_file"),
        F.col("_ingested_at"),
    )


def clean_attendance(dataframe, include_quality_columns: bool = False):
    discharge_flag = try_integral("discharge_flag", "INT")
    columns = [
        try_integral("attendance_id", "BIGINT").alias("attendance_id"),
        try_integral("patient_id", "BIGINT").alias("patient_id"),
        try_integral("doctor_id", "BIGINT").alias("doctor_id"),
        try_integral("hospital_id", "BIGINT").alias("hospital_id"),
        try_integral("disease_id", "BIGINT").alias("disease_id"),
        try_cast("attendance_date", "TIMESTAMP").alias("attendance_timestamp"),
        try_cast("attendance_date", "DATE").alias("attendance_date"),
        try_integral("wait_time_minutes", "INT").alias("wait_time_minutes"),
        try_cast("cost", "DECIMAL(12,2)").alias("cost"),
        try_integral("severity_score", "INT").alias("severity_score"),
    ]
    if include_quality_columns:
        columns.append(discharge_flag.alias("discharge_flag"))
    columns.extend(
        [
            (discharge_flag == 1).alias("is_discharged"),
            try_cast("created_at", "TIMESTAMP").alias("created_at"),
            F.col("odate"),
            F.col("_source_file"),
            F.col("_ingested_at"),
        ]
    )
    return dataframe.select(*columns)


CLEANERS = {
    "patients": clean_patients,
    "hospitals": clean_hospitals,
    "doctors": clean_doctors,
    "diseases": clean_diseases,
}


def clean_table(dataframe, table_name: str, include_quality_columns: bool = False):
    """Deduplicate, type, normalize and remove incomplete non-key records."""
    if table_name not in TABLE_KEYS:
        raise ValueError(f"Unsupported Bronze table: {table_name}")

    deduplicated = deduplicate_snapshot(dataframe, TABLE_KEYS[table_name])
    if table_name == "attendance":
        cleaned = clean_attendance(deduplicated, include_quality_columns)
    else:
        cleaned = CLEANERS[table_name](deduplicated)

    return cleaned.na.drop(subset=CLEANUP_DROP_NULLS[table_name])
