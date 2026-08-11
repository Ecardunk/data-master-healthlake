import pytest

import main as generator_main


def test_clean_profile_disables_new_anomalies():
    clean = generator_main.quality_kwargs("patients", "clean")
    chaos = generator_main.quality_kwargs("patients", "chaos")

    assert clean == {"null_percentages": {}, "duplicate_percentage": 0}
    assert chaos["null_percentages"]
    assert chaos["duplicate_percentage"] > 0


def test_existing_partition_is_not_overwritten_by_default(tmp_path):
    partition = tmp_path / "odate=2026-08-07"
    partition.mkdir()
    (partition / "patients.csv").write_text("patient_id\n1\n", encoding="utf-8")

    with pytest.raises(FileExistsError):
        generator_main.validate_output_partition(partition, overwrite=False)
