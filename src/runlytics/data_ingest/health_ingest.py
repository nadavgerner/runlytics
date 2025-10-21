# src/runlytics/data_ingest/health_ingest.py
import json
import pandas as pd
from pathlib import Path


def load_health_json(filepath):
    """Load one Apple Health JSON file."""
    with open(filepath, "r") as f:
        data = json.load(f)
    return data


def extract_metrics(data, filename=None):
    """
    Return the list of metric dictionaries from any known schema variant.
    Handles:
    - data["metrics"]
    - data["data"]["metrics"]
    - data["data"] (if it's already a list)
    """
    if "metrics" in data:
        return data["metrics"]

    if "data" in data:
        # Case 1: data["data"] is dict with metrics
        if isinstance(data["data"], dict) and "metrics" in data["data"]:
            return data["data"]["metrics"]

        # Case 2: data["data"] itself is a list of metrics
        if isinstance(data["data"], list):
            return data["data"]

    print(f"Skipping {filename or 'unknown file'} — no recognized metric structure.")
    return []


def flatten_metrics(metrics_list, filename=None):
    """Flatten all metrics into a single DataFrame."""
    records = []
    for metric in metrics_list:
        name = metric.get("name")
        unit = metric.get("units")
        for entry in metric.get("data", []):
            records.append({
                "metric": name,
                "unit": unit,
                "date": entry.get("date"),
                "qty": entry.get("qty"),
                "source": entry.get("source"),
                "Min": entry.get("Min"),
                "Avg": entry.get("Avg"),
                "Max": entry.get("Max"),
            })
    df = pd.DataFrame(records)
    if df.empty:
        print(f"No data rows found in {filename or 'file'}")
    return df


def process_health_folder(folder="/mnt/c/Users/nadav/iCloudDrive/iCloud~com~ifunography~HealthExport/Runlytics"):
    """Ingest all valid HealthAutoExport JSON files and save to Parquet."""
    path = Path(folder)
    files = sorted(path.glob("HealthAutoExport-*.json"))
    if not files:
        raise FileNotFoundError(f"No HealthAutoExport-*.json files in {folder}")

    dfs = []
    for file in files:
        try:
            print(f"Processing {file.name}")
            data = load_health_json(file)
            metrics = extract_metrics(data, filename=file.name)
            if not metrics:
                continue
            df = flatten_metrics(metrics, filename=file.name)
            if not df.empty:
                df["source_file"] = file.name
                dfs.append(df)
        except Exception as e:
            print(f"Failed to process {file.name}: {e}")

    if not dfs:
        raise RuntimeError("No valid Apple Health files processed.")

    final_df = pd.concat(dfs, ignore_index=True)
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    final_df.to_parquet("data/raw/apple_health.parquet", index=False)
    print(f"Saved {len(final_df)} Apple Health records to data/raw/apple_health.parquet")


if __name__ == "__main__":
    process_health_folder()
