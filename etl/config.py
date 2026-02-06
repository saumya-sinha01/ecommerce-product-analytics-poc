# This script is the Configuration Engine of the project. It acts as the
# "Brain" of the ETL pipeline, converting a static base.yaml file into a strictly 
# structured Python object that the rest of the application uses to find data, 
# connect to servers, and apply business rules.

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict
import yaml

def load_yaml(path: str | Path) -> Dict[str, Any]:
    """
    A helper utility to safely read a YAML file from the disk.
    Includes error handling for missing files.
    """
    p = Path(path)
    if not p.exists():
        # resolve() gives the full absolute path to make debugging easier
        raise FileNotFoundError(f"Config not found: {p.resolve()}")
    with p.open("r", encoding="utf-8") as f:
        # returns an empty dict if the file is empty
        return yaml.safe_load(f) or {}

@dataclass(frozen=True)
class S3Config:
    """
    A structured container for Cloud Storage settings.
    'frozen=True' makes it read-only, preventing accidental changes at runtime.
    """
    endpoint_url: str
    region: str
    access_key: str
    secret_key: str
    raw_bucket: str
    processed_bucket: str
    raw_prefix: str
    processed_prefix: str

@dataclass(frozen=True)
class AppConfig:
    """
    The Master Configuration object. 
    Groups all sub-configs (S3, ETL, Mart) into one easy-to-pass object.
    """
    s3: S3Config
    paths: Dict[str, Any]
    schema: Dict[str, Any]
    experiment: Dict[str, Any]
    etl: Dict[str, Any]
    mart: Dict[str, Any]

def load_config(path: str | Path = "config/base.yaml") -> AppConfig:
    """
    The main entry point for configuration logic.
    It reads the raw YAML and maps it to the Python classes above.
    """
    cfg = load_yaml(path)

    # Navigate the nested YAML structure: storage -> s3
    s3_cfg = cfg.get("storage", {}).get("s3", {})
    
    # Validation: Ensure all critical S3 keys are present before starting
    required = [
        "endpoint_url", "region", "access_key", "secret_key",
        "raw_bucket", "processed_bucket", "raw_prefix", "processed_prefix"
    ]
    missing = [k for k in required if k not in s3_cfg]
    if missing:
        raise ValueError(f"Missing keys in storage.s3: {missing}")

    # Initialize the S3Config object
    # .strip("/") ensures paths don't have messy double slashes (e.g., bucket//folder)
    s3 = S3Config(
        endpoint_url=str(s3_cfg["endpoint_url"]),
        region=str(s3_cfg["region"]),
        access_key=str(s3_cfg["access_key"]),
        secret_key=str(s3_cfg["secret_key"]),
        raw_bucket=str(s3_cfg["raw_bucket"]),
        processed_bucket=str(s3_cfg["processed_bucket"]),
        raw_prefix=str(s3_cfg["raw_prefix"]).strip("/"),
        processed_prefix=str(s3_cfg["processed_prefix"]).strip("/"),
    )

    # Return the unified AppConfig object containing all sections
    return AppConfig(
        s3=s3,
        paths=cfg.get("paths", {}),
        schema=cfg.get("schema", {}),
        experiment=cfg.get("experiment", {}),
        etl=cfg.get("etl", {}),
        mart=cfg.get("mart", {}),
    )