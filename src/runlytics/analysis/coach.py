import os
import json
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

class AICoach:
    def __init__(self):
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            raise ValueError("DATABASE_URL environment variable is not set")
        
        # Connect to Supabase
        self.engine = create_engine(db_url, pool_pre_ping=True)

    def get_metric_stats(self, df, metric_name, days=30):
        """Helper to safely calculate stats for a specific metric."""
        sub_df = df[df['type'] == metric_name]
        
        if sub_df.empty:
            return None

        # 7-day average (Current Status)
        recent_df = sub_df[sub_df['date'] > (datetime.now() - pd.Timedelta(days=7))]
        recent_avg = recent_df['value'].mean()
        
        # 30-day average (Baseline)
        overall_avg = sub_df['value'].mean()
        
        # Avoid division by zero
        if pd.isna(recent_avg): recent_avg = 0
        if pd.isna(overall_avg) or overall_avg == 0: overall_avg = 1 

        return {
            "7_day_avg": round(recent_avg, 2),
            "30_day_baseline": round(overall_avg, 2),
            "trend": "High" if recent_avg > overall_avg * 1.02 else "Low" if recent_avg < overall_avg * 0.98 else "Stable"
        }

    def get_physiology(self, days=30):
        """Calculates cardio and body composition baselines."""
        # UPDATED: Added 'weight_body_mass' and 'lean_body_mass' to the query
        query = f"""
        SELECT type, value, date
        FROM biometrics
        WHERE date > NOW() - INTERVAL '{days} days'
        AND type IN (
            'resting_heart_rate', 'heart_rate_variability', 'vo2_max', 
            'apple_exercise_time', 'weight_body_mass', 'body_fat_percentage', 'lean_body_mass'
        )
        """
        df = pd.read_sql(query, self.engine)

        if df.empty:
            return {"error": "No biometrics found"}

        # Normalize names to be cleaner for the AI
        # 'weight_body_mass' -> 'weight'
        df['type'] = df['type'].replace('weight_body_mass', 'weight')

        stats = {
            "cardio": {},
            "composition": {}
        }

        # Process Cardio Metrics
        for m in ['resting_heart_rate', 'heart_rate_variability', 'vo2_max']:
            res = self.get_metric_stats(df, m)
            if res: stats['cardio'][m] = res

        # Process Body Composition (Updated for your data)
        for m in ['weight', 'body_fat_percentage', 'lean_body_mass']:
            res = self.get_metric_stats(df, m)
            if res: stats['composition'][m] = res

        return stats

    def get_recent_runs(self, limit=10):
        """Fetches recent run data."""
        query = f"""
        SELECT date, distance_km, duration_min, avg_hr, energy_kcal
        FROM runs
        ORDER BY date DESC
        LIMIT {limit}
        """
        df = pd.read_sql(query, self.engine)
        if df.empty: return []
        df['date'] = df['date'].astype(str)
        return df.to_dict(orient='records')

    def generate_report(self):
        """Generates the context file for the LLM."""
        physio = self.get_physiology()
        runs = self.get_recent_runs()

        report = {
            "generated_at": datetime.now().isoformat(),
            "athlete_goals": {
                "primary": "Sub-3 Hour Marathon (Endurance/Efficiency)",
                "additional_note": "Balance high-mileage cardio with some explosive power maintenance. Monitor weight for power-to-weight ratio."
            },
            "physiology": physio,
            "recent_training": runs,
            "analysis_instructions": "Analyze the user's readiness. If HRV is low, warn against high-intensity plyometrics. If Weight is trending up, flag it as a risk for both goals."
        }

        # Save locally
        output_path = "data/coach_context.json"
        os.makedirs("data", exist_ok=True)
        
        with open(output_path, "w") as f:
            json.dump(report, f, indent=2)
        
        return output_path

if __name__ == "__main__":
    try:
        coach = AICoach()
        file_path = coach.generate_report()
        print(f"Coach V2 Report Generated: {file_path}")
    except Exception as e:
        print(f"Execution failed: {e}")