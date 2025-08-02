# UFO and Drone Sightings Analysis
# By: A college student learning data analysis

import pandas as pd
import json

# === 1. Load the UFO Dataset ===
ufo_path = r'C:\Users\opiej\ufo-analysis-capstone\ufo-analysis-capstone\data\ufo_sightings.json'
with open(ufo_path) as file:
    ufo_data = json.load(file)

# Flatten JSON
ufo_df = pd.json_normalize(ufo_data['rows'])

# Clean and convert 'Occurred'
ufo_df['Occurred'] = pd.to_datetime(
    ufo_df['row'].apply(lambda r: r.get('Occurred', None)).astype(str).str.replace('Local', '').str.strip(),
    errors='coerce'
)

# Drop nulls
ufo_df = ufo_df.dropna(subset=['Occurred'])

# Extract city and state from 'Location'
location_series = ufo_df['row'].apply(lambda r: r.get('Location', ''))
ufo_df['City'], ufo_df['State'] = location_series.str.extract(r'^(.*?),\s*([A-Z]{2})').T.values

# Extract date parts
ufo_df['date_only'] = ufo_df['Occurred'].dt.date
ufo_df['year'] = ufo_df['Occurred'].dt.year
ufo_df['month'] = ufo_df['Occurred'].dt.month
ufo_df['hour'] = ufo_df['Occurred'].dt.hour

# === 2. Load the Drone Dataset ===
drone_path = r'C:\Users\opiej\ufo-analysis-capstone\ufo-analysis-capstone\data\drone_sightings.csv'
drone_df = pd.read_csv(drone_path)

# Clean city/state and dates
drone_df['City'] = drone_df['City'].str.strip().str.title()
drone_df['State'] = drone_df['State'].str.strip().str.upper()
drone_df['datetime'] = pd.to_datetime(drone_df['Date'], errors='coerce')
drone_df = drone_df.dropna(subset=['datetime'])
drone_df['date_only'] = drone_df['datetime'].dt.date

# === 3. Prepare UFO fields for merging ===
ufo_df['City'] = ufo_df['City'].str.strip().str.title()
ufo_df['State'] = ufo_df['State'].str.strip().str.upper()

# === 4. Merge UFO and Drone Sightings ===
merged_df = pd.merge(
    ufo_df,
    drone_df,
    on=['date_only', 'City', 'State'],
    how='inner'
)

# === 5. Preview Merged Data ===
print("Merged rows:", len(merged_df))
merged_df[['Occurred', 'City', 'State', 'Summary', 'datetime']].head()
