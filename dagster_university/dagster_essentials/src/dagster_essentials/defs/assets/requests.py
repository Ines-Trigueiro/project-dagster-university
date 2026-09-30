# src/dagster_essentials/defs/assets/requests.py
import dagster as dg

# Config is used as the base class when making custom configurations
class AdhocRequestConfig(dg.Config):
    filename: str
    borough: str
    start_date: str
    end_date: str
