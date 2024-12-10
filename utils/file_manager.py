from typing import Any, Dict
import json
import os

def save_data_to_file(data: Dict[str, Any], file_path: str) -> None:
    """
    Save given data dictionary as JSON file to the specified file path.

    :param data: Dictionary to save.
    :param file_path: File path (directories will be created if not present).
    """
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
