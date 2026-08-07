import ast
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
BRONZE_INGESTION = REPO_ROOT / "databricks" / "src" / "bronze" / "ingestion.py"


def assigned_literal(path: Path, variable_name: str):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if any(
            isinstance(target, ast.Name) and target.id == variable_name
            for target in node.targets
        ):
            return ast.literal_eval(node.value)
    raise AssertionError(f"{variable_name} was not assigned in {path}")


def assigned_string(path: Path, variable_name: str) -> str:
    value = assigned_literal(path, variable_name)
    assert isinstance(value, str)
    return value


def test_bronze_odate_regex_matches_the_adf_path_contract():
    pattern = assigned_string(BRONZE_INGESTION, "ODATE_PATH_PATTERN")

    match = re.search(
        pattern,
        "abfss://raw@storage.dfs.core.windows.net/patients/"
        "odate=2026-08-06/patients.csv",
    )

    assert match is not None
    assert match.group(1) == "2026-08-06"
    assert re.search(pattern, "/patients/not-odate=2026-08-06/patients.csv") is None


def test_medallion_layers_do_not_silently_drop_failed_rows():
    pipeline_sources = [
        BRONZE_INGESTION,
        REPO_ROOT / "databricks" / "src" / "silver" / "transforms.py",
        REPO_ROOT / "databricks" / "src" / "gold" / "marts.py",
    ]

    for source in pipeline_sources:
        assert "expect_or_drop" not in source.read_text(encoding="utf-8")


def test_gate_requires_odate_and_applies_cleaning_before_checks():
    source = (
        REPO_ROOT / "databricks" / "src" / "dq" / "quality_gate.py"
    ).read_text(encoding="utf-8")

    assert 'parser.add_argument("--odate", required=True' in source
    assert "source_df = with_effective_odate(source_df)" in source
    assert "source_for_odate = source_df.where(" in source
    assert "checked_df = prepare_for_checks(source_for_odate" in source
    assert source.index("checked_df = prepare_for_checks") < source.index(
        "dq_engine.apply_checks_and_split"
    )
    assert ".cache()" not in source
    assert ".persist()" not in source
    assert ".saveAsTable(source_table)" not in source


def test_silver_and_gold_read_only_gate_approved_snapshots():
    silver_source = (
        REPO_ROOT / "databricks" / "src" / "silver" / "transforms.py"
    ).read_text(encoding="utf-8")
    gold_source = (
        REPO_ROOT / "databricks" / "src" / "gold" / "marts.py"
    ).read_text(encoding="utf-8")

    assert 'F.col("dq_stage") == "bronze_to_silver"' in silver_source
    assert 'F.col("dq_stage") == "silver_to_gold"' in gold_source
    assert "approved_silver(" in gold_source


def test_cleaning_removes_only_incomplete_non_key_records_before_dq():
    cleaning_source = (
        REPO_ROOT / "databricks" / "src" / "silver" / "cleaning.py"
    )
    required_columns = assigned_literal(cleaning_source, "CLEANUP_DROP_NULLS")

    assert required_columns == {
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
