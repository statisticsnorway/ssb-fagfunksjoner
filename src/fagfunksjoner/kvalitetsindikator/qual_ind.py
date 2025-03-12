
import json
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path

from jsonschema import validate, ValidationError

# Define the expected schema
QUALITY_INDICATOR_SCHEMA = {
    "type": "object",
    "properties": {
        "title": {"type": "string"},
        "description": {"type": "string"},
        "type": {"type": "string", "enum": ["koblingsrate", "filter", "other"]},  # Enforcing predefined types
        "unit": {"type": "string", "enum": ["percent", "count", "other"]},  # Restrict to known units
        "data_period": {"type": "string", "pattern": r"^\d{4}-\d{2}$"},  # YYYY-MM format
        "data_source": {
            "type": "array",
            "items": {"type": "string"}
        },
        "value": {}  # Can be any type (NaN, float, etc.)
    },
    "required": ["title", "description", "type", "unit", "data_period", "data_source", "value"]
}

class QualIndLogger:
    def __init__(self, 
                 log_dir: str | Path, 
                 year: int| str, 
                 month: str | int):
        """
        Initializes the QualityLogger.
        
        :param log_dir: Directory where logs are stored.
        """
        self.log_dir = Path(log_dir)
        self.year = int(year)
        self.month = str(month).zfill(2)

        os.makedirs(self.log_dir, exist_ok=True)  # Ensure log directory exists
        self.log_file = self.log_dir / f"prosess_data_p{self.year}-{self.month}.json"
        self.indicators = {}
        # ensuring appending indicators accross sessions in same pipeline (produksjonsløp)
        if self.log_file.exists():
            self.indicators = self.get_logs(year, month)

    def validate_indicator(self, indicator_data):
        """Validates that the indicator matches the predefined schema."""
        try:
            validate(instance=indicator_data, schema=QUALITY_INDICATOR_SCHEMA)
        except ValidationError as e:
            raise ValueError(f"Invalid indicator format: {e.message}")

    def log_indicator(self, key, indicator_data):
        """Logs a new quality indicator after validating its format."""
        self.validate_indicator(indicator_data)  # Ensure format is correct
        self.indicators[key] = indicator_data
        self._save_logs()

    def update_indicator_value(self, indicator, key, value):
        """Updates the value of an existing indicator."""
        if key in self.indicators[indicator]:
            self.indicators[indicator][key] = value
            self._save_logs()
        else:
            print(f"Warning: Indicator {indicator}-key '{key}' not found in the current log.")

    def _save_logs(self):
        """Saves the indicators to the current month's log file."""
        with open(self.log_file, "w", encoding="utf-8") as file:
            json.dump(self.indicators, file, indent=4, ensure_ascii=False)

    def get_logs(self, year, month):
        """Retrieves logs for a specific month."""
        month = str(month).zfill(2)
        log_path = self.log_dir / f"prosess_data_p{year}-{month}.json"
        if os.path.exists(log_path):
            with open(log_path, "r", encoding="utf-8") as file:
                return json.load(file)
        return {}
    # change to periode handeling. pYYYY-MM / pYYYY-Qx(?)
    def compare_months(self, indicator, n_periods = 5):
        """Compares quality indicators across the given months."""
        periods = [datetime(self.year, int(self.month), 1) - relativedelta(months=i) for i in range(n_periods)]
        month_logs = {f"{date.year}-{str(date.month).zfill(2)}": self.get_logs(date.year, date.month) for date in periods}
        comparison = {}
        
        print("Comparison across months:")
        print(f"\nIndicator: {indicator}")
        for date, logs in month_logs.items():
            for key, data in logs.items():
                if key not in comparison:
                    comparison[key] = {}
                print(f"  {date}: {data['value']} {data['unit']}")
