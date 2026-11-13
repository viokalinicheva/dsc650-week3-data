"""Parameterize the required instructor flow without replacing its processors."""

import argparse
import hashlib
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    original = args.source.read_bytes()
    expected_hash = "3db0c1e3a6f1db6c627b5f3ecf641c229cbb9b6918b45fafc2dd02daf9870414"
    if hashlib.sha256(original).hexdigest() != expected_hash:
        raise ValueError("The supplied template does not match my preserved original.")
    flow = json.loads(original)
    old_name = flow["flowContents"]["parameterContextName"]
    context = flow["parameterContexts"].pop(old_name)
    new_name = "Concrete Strength Final Project"
    context["name"] = new_name
    values = {
        "DOWNLOAD FILE URL": "https://raw.githubusercontent.com/viokalinicheva/dsc650-week3-data/main/final_project/data/concrete_strength.csv",
        "FILENAME": "concrete_strength.csv",
        "HDFS WRITE DIRECTORY": "/user/violettakalinicheva/dsc650_final/raw",
        "USER": "violettakalinicheva",
    }
    for parameter in context["parameters"]:
        parameter["value"] = values[parameter["name"]]
    flow["parameterContexts"][new_name] = context
    flow["flowContents"]["parameterContextName"] = new_name
    flow["flowContents"]["name"] = new_name
    flow["flowContents"]["comments"] = (
        "I adapted the required Final_Project.json for a new UCI concrete dataset. "
        "I run the download once, check the queues and HDFS file, then stop NiFi."
    )
    # I leave the supplied processors and relationships unchanged.
    assert len(flow["flowContents"]["processors"]) == 3
    assert len(flow["flowContents"]["connections"]) == 4
    args.output.parent.mkdir(parents=True, exist_ok=True)
    if args.output.exists():
        raise FileExistsError("Refusing to replace an existing configured flow.")
    args.output.write_text(json.dumps(flow, indent=2) + "\n", encoding="utf-8")
    print("Configured the required template:", args.output)


if __name__ == "__main__":
    main()
