import pandas as pd
from datetime import datetime

class HealthParser:
    def __init__(self, payload):
        self.payload = payload

    def _extract_list(self, key_name):
        """
        Your original robust logic to find lists (metrics or workouts)
        regardless of how the JSON is nested.
        """
        # Case 1: Top level (e.g., payload['metrics'])
        if key_name in self.payload:
            return self.payload[key_name]
        
        # Case 2: Nested in 'data' (e.g., payload['data']['metrics'])
        data_block = self.payload.get("data", {})
        if isinstance(data_block, dict) and key_name in data_block:
            return data_block[key_name]
            
        # Case 3: 'data' is the list itself (rare, but your code handled it)
        if key_name == "metrics" and isinstance(data_block, list):
            return data_block

        return []

    def parse_biometrics(self):
        """Extracts background metrics like HR, Steps, HRV."""
        metrics = self._extract_list("metrics")
        parsed_data = []

        for m in metrics:
            name = m.get("name")
            unit = m.get("units")
            
            # Some exports use 'data', some might use 'qty' directly if aggregated
            points = m.get("data", [])
            
            for point in points:
                # Clean the date string: "2025-12-10 08:32:00 -0500" -> UTC naive
                date_str = point.get("date")
                try:
                    # Parse and strip timezone for Postgres compatibility
                    dt_obj = pd.to_datetime(date_str).replace(tzinfo=None)
                except:
                    continue

                parsed_data.append({
                    "date": dt_obj,
                    "type": name,
                    "value": point.get("qty"),
                    "unit": unit,
                    "source": point.get("source", "Apple Health")
                })
        
        return parsed_data

    def parse_workouts(self):
        """Extracts running workouts."""
        workouts = self._extract_list("workouts")
        parsed_runs = []

        for w in workouts:
            # We only care about Running
            if w.get("name") == "Running":
                date_str = w.get("start")
                try:
                    dt_obj = pd.to_datetime(date_str).replace(tzinfo=None)
                except:
                    continue

                # Create a unique ID based on time hash to prevent duplicates
                run_id = f"run_{int(dt_obj.timestamp())}"

                parsed_runs.append({
                    "id": run_id,
                    "date": dt_obj,
                    "duration_min": w.get("duration"),
                    "distance_km": w.get("distance"),
                    "avg_hr": w.get("avg_heart_rate"),
                    "max_hr": w.get("max_heart_rate"),
                    "energy_kcal": w.get("active_energy"),
                    "source": "Apple Health",
                    "route_json": w.get("route", [])
                })
        
        return parsed_runs