import sys
from datetime import date

from jobs.extreme_weather import run_job

if __name__ == "__main__":
    """
    Usage: extreme-weather [year]
    Displays extreme weather stats (highest temperature, wind, precipitation) for the given, or latest, year.
    """
    run_job()
