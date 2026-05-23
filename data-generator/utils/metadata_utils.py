import json

from config.settings import METADATA_FILE


def load_metadata():

    with open(METADATA_FILE, "r", encoding="utf-8") as file:
        return json.load(file)


def save_metadata(metadata):
    temp_file = METADATA_FILE.with_suffix(".tmp")

    with open(temp_file, "w", encoding="utf-8") as file:
        json.dump(
            metadata,
            file,
            indent=4
        )
        file.write("\n")

    temp_file.replace(METADATA_FILE)
