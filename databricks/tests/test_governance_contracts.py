import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
ACCESS_SQL = (
    REPO_ROOT / "databricks" / "src" / "governance" / "unity_catalog_access.sql"
)

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


def test_only_the_functional_admin_can_read_bronze():
    bronze_principals = {
        principal
        for principal, object_type, object_name, privileges in parse_grants()
        if object_type == "SCHEMA"
        and object_name.endswith(".bronze")
        and "SELECT" in privileges
    }

    assert bronze_principals == {"data-engineering-admin"}
