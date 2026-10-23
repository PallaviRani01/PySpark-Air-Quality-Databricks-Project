"""
Configuration and constants for the Air Quality PySpark Databricks project.
"""

# UCI Dataset Schema Definition
RAW_COLUMNS = [
    "Date", "Time", "CO(GT)", "PT08.S1(CO)", "NMHC(GT)", "C6H6(GT)",
    "PT08.S2(NMHC)", "NOx(GT)", "PT08.S3(NOx)", "NO2(GT)", "PT08.S4(NO2)",
    "PT08.S5(O3)", "T", "RH", "AH", "_c15", "_c16"
]

CLEAN_COLUMN_MAPPING = {
    "date_raw": "date_str",
    "time_raw": "time_str",
    "co_gt": "co_gt",                      # True hourly averaged CO in mg/m^3 (reference analyzer)
    "sensor_s1_co": "sensor_s1_co",        # PT08.S1 (tin oxide) sensor response (nominally CO targeted)
    "nmhc_gt": "nmhc_gt",                  # Non-Methane HydroCarbons concentration in microg/m^3
    "c6h6_gt": "c6h6_gt",                  # True hourly averaged Benzene concentration in microg/m^3
    "sensor_s2_nmhc": "sensor_s2_nmhc",    # PT08.S2 (titania) sensor response (nominally NMHC targeted)
    "nox_gt": "nox_gt",                    # True hourly averaged NOx concentration in ppb
    "sensor_s3_nox": "sensor_s3_nox",      # PT08.S3 (tungsten oxide) sensor response (nominally NOx targeted)
    "no2_gt": "no2_gt",                    # True hourly averaged NO2 concentration in microg/m^3
    "sensor_s4_no2": "sensor_s4_no2",      # PT08.S4 (tungsten oxide) sensor response (nominally NO2 targeted)
    "sensor_s5_o3": "sensor_s5_o3",        # PT08.S5 (indium oxide) sensor response (nominally O3 targeted)
    "temperature": "temperature",          # Temperature in °C
    "relative_humidity": "relative_humidity", # Relative Humidity (%)
    "absolute_humidity": "absolute_humidity"  # Absolute Humidity
}

ORDERED_CLEAN_NAMES = [
    "date_str", "time_str", "co_gt", "sensor_s1_co", "nmhc_gt", "c6h6_gt",
    "sensor_s2_nmhc", "nox_gt", "sensor_s3_nox", "no2_gt", "sensor_s4_no2",
    "sensor_s5_o3", "temperature", "relative_humidity", "absolute_humidity"
]

DECIMAL_COLUMNS = ["co_gt", "c6h6_gt", "temperature", "relative_humidity", "absolute_humidity"]

INTEGER_COLUMNS = [
    "sensor_s1_co", "nmhc_gt", "sensor_s2_nmhc", "nox_gt",
    "sensor_s3_nox", "no2_gt", "sensor_s4_no2", "sensor_s5_o3"
]

ALL_METRIC_COLUMNS = DECIMAL_COLUMNS + INTEGER_COLUMNS

MISSING_VALUE_SENTINEL = -200.0

# European Union (EU) & WHO Air Quality Standards (Directives 2008/50/EC)
EU_AIR_QUALITY_LIMITS = {
    "NO2_HOURLY_LIMIT": 200.0,    # 200 ug/m3 (hourly limit not to be exceeded > 18 hours/year)
    "CO_8H_MAX_LIMIT": 10.0,      # 10 mg/m3 (maximum daily 8-hour mean)
    "C6H6_ANNUAL_LIMIT": 5.0      # 5 ug/m3 (annual calendar year limit)
}

# Traffic dynamics
MORNING_RUSH_HOURS = [8, 9]
EVENING_RUSH_HOURS = [18, 19, 20]
ALL_RUSH_HOURS = MORNING_RUSH_HOURS + EVENING_RUSH_HOURS
