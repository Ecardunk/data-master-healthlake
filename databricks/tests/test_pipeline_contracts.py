import ast
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
BRONZE_INGESTION = REPO_ROOT / "databricks" / "src" / "bronze" / "ingestion.py"
BATCH_HELPERS = REPO_ROOT / "databricks" / "src" / "common" / "batch.py"


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


def test_bronze_odate_regex_matches_the_adf_path_contract():
    pattern = assigned_literal(BRONZE_INGESTION, "ODATE_PATH_PATTERN")

    match = re.search(
        pattern,
        "abfss://raw@storage.dfs.core.windows.net/patients/"
        "odate=2026-08-06/patients.csv",
    )

    assert match is not None
    assert match.group(1) == "2026-08-06"
    assert re.search(pattern, "/patients/not-odate=2026-08-06/patients.csv") is None


def test_entity_table_names_are_the_same_in_every_medallion_layer():
    names = assigned_literal(BATCH_HELPERS, "TABLE_NAMES")
    cleaning = (
        REPO_ROOT / "databricks" / "src" / "silver" / "cleaning.py"
    ).read_text(encoding="utf-8")

    assert names == (
        "patients",
        "hospitals",
        "doctors",
        "diseases",
        "attendance",
    )
    assert 'F.col("odate")' in cleaning
