import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
ACCESS_SQL = (
    REPO_ROOT / "databricks" / "src" / "governance" / "unity_catalog_access.sql"
)

TARGET_GROUPS = {
    "data-engineering-admin",
    "data-engineering",
    "data-analysts",
    "data-scientists",
    "power-bi",
}

LEGACY_GROUPS = {
    "healthlake-dev-bi-readers",
    "healthlake-dev-data-analysts",
    "healthlake-dev-data-engineers-readers",
    "healthlake-dev-data-engineers-contributors",
    "healthlake-prod-bi-readers",
    "healthlake-prod-data-analysts",
    "healthlake-prod-data-engineers-readers",
}

GRANT_PATTERN = re.compile(
    r"GRANT\s+(?P<privileges>[A-Z ,_]+?)\s+"
    r"ON\s+(?P<object_type>CATALOG|SCHEMA)\s+"
    r"(?P<object_name>[a-z0-9_.]+)\s+"
    r"TO\s+`(?P<principal>[^`]+)`\s*;",
    re.IGNORECASE,
)


def parse_grants():
    source = ACCESS_SQL.read_text(encoding="utf-8")
    grants = set()
    for match in GRANT_PATTERN.finditer(source):
        privileges = frozenset(
            privilege.strip().upper()
            for privilege in match.group("privileges").split(",")
        )
        grants.add(
            (
                match.group("principal"),
                match.group("object_type").upper(),
                match.group("object_name"),
                privileges,
            )
        )
    return grants


def expected_grants():
    expected = set()

    def catalog(principal, catalog_name):
        expected.add(
            (principal, "CATALOG", catalog_name, frozenset({"USE CATALOG"}))
        )

    def schema(principal, schema_name):
        expected.add(
            (
                principal,
                "SCHEMA",
                schema_name,
                frozenset({"USE SCHEMA", "SELECT"}),
            )
        )

    catalog("data-engineering-admin", "healthlake_dev")
    for layer in ("bronze", "silver", "gold"):
        schema("data-engineering-admin", f"healthlake_dev.{layer}")

    catalog("data-engineering", "healthlake_dev")
    for layer in ("silver", "gold"):
        schema("data-engineering", f"healthlake_dev.{layer}")

    for principal in TARGET_GROUPS:
        catalog(principal, "healthlake_prod")
        for layer in ("silver", "gold"):
            schema(principal, f"healthlake_prod.{layer}")

    schema("data-engineering-admin", "healthlake_prod.bronze")
    return expected


def test_unity_catalog_grants_match_the_human_access_matrix_exactly():
    assert parse_grants() == expected_grants()


def test_only_the_functional_admin_can_read_bronze():
    bronze_principals = {
        principal
        for principal, object_type, object_name, privileges in parse_grants()
        if object_type == "SCHEMA"
        and object_name.endswith(".bronze")
        and "SELECT" in privileges
    }

    assert bronze_principals == {"data-engineering-admin"}


def test_human_groups_never_receive_write_or_admin_privileges():
    allowed_privileges = {"USE CATALOG", "USE SCHEMA", "SELECT"}

    assert {
        principal for principal, *_ in parse_grants()
    } == TARGET_GROUPS
    for _, _, _, privileges in parse_grants():
        assert privileges <= allowed_privileges


def test_legacy_group_names_are_not_part_of_the_declared_governance():
    governed_files = [
        ACCESS_SQL,
        REPO_ROOT / "README.md",
        REPO_ROOT / "databricks" / "README.md",
    ]
    combined = "\n".join(
        path.read_text(encoding="utf-8") for path in governed_files
    )

    for legacy_group in LEGACY_GROUPS:
        assert legacy_group not in combined
