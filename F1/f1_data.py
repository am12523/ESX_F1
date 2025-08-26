import requests
import pandas as pd
import os

API_KEY = "dbd215da05696b0174d908a68c8d76e9"
BASE_URL = "https://v1.formula-1.api-sports.io"
HEADERS = {"x-apisports-key": API_KEY}

os.makedirs("f1_data_csv", exist_ok=True)

def fetch(endpoint, params=None):
    """Helper function to fetch data from API."""
    url = f"{BASE_URL}/{endpoint}"
    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=10)
        response.raise_for_status()
        return response.json().get("response", [])
    except Exception as e:
        print(f" Error fetching {endpoint} {params}: {e}")
        return []

def normalize_with_meta(records, meta):
    """Flatten JSON and add metadata like season, race_id."""
    if not records:
        return pd.DataFrame()
    df = pd.json_normalize(records)
    for k, v in meta.items():
        df[k] = v
    return df

#Getting last 3 seasons
seasons = fetch("seasons")
seasons = sorted([s for s in seasons if isinstance(s, int)], reverse=True)[:3]
print(f"📅 Using seasons: {seasons}")

# Stoage dicts for merging
drivers_all, races_all = [], []
rankings_teams_all, rankings_drivers_all = [], []
pitstops_all, flaps_all, grid_all, race_rankings_all = [], [], [], []

#General endpoints (no params)
for ep in ["status", "timezone", "competitions", "circuits", "teams"]:
    data = fetch(ep)
    if data:
        df = pd.json_normalize(data)
        df.to_csv(f"f1_data_csv/{ep}.csv", index=False)
        print(f"Saved {ep} ({len(df)} rows)")

# Season-specific endpoints
for season in seasons:
    print(f"\n📌 Season {season}")

    # Drivers
    drivers = fetch("drivers", {"season": season})
    df = normalize_with_meta(drivers, {"season": season})
    if not df.empty: drivers_all.append(df)

    # Races
    races = fetch("races", {"season": season})
    df = normalize_with_meta(races, {"season": season})
    if not df.empty: races_all.append(df)

    # Team rankings
    rankings_teams = fetch("rankings/teams", {"season": season})
    df = normalize_with_meta(rankings_teams, {"season": season})
    if not df.empty: rankings_teams_all.append(df)

    # Driver rankings
    rankings_drivers = fetch("rankings/drivers", {"season": season})
    df = normalize_with_meta(rankings_drivers, {"season": season})
    if not df.empty: rankings_drivers_all.append(df)

    # Race-specific data
    for race in races:
        race_id = race.get("id")
        if not race_id:
            continue

        pitstops = fetch("pitstops", {"race": race_id})
        df = normalize_with_meta(pitstops, {"season": season, "race_id": race_id})
        if not df.empty: pitstops_all.append(df)

        flaps = fetch("rankings/fastestlaps", {"race": race_id})
        df = normalize_with_meta(flaps, {"season": season, "race_id": race_id})
        if not df.empty: flaps_all.append(df)

        grid = fetch("rankings/startinggrid", {"race": race_id})
        df = normalize_with_meta(grid, {"season": season, "race_id": race_id})
        if not df.empty: grid_all.append(df)

        race_rankings = fetch("rankings/races", {"race": race_id})
        df = normalize_with_meta(race_rankings, {"season": season, "race_id": race_id})
        if not df.empty: race_rankings_all.append(df)

# Merging and saving
def merge_and_save(dfs, name):
    if dfs:
        merged = pd.concat(dfs, ignore_index=True)
        merged.to_csv(f"f1_data_csv/{name}.csv", index=False)
        print(f" Saved merged {name} ({len(merged)} rows)")

merge_and_save(drivers_all, "drivers")
merge_and_save(races_all, "races")
merge_and_save(rankings_teams_all, "rankings_teams")
merge_and_save(rankings_drivers_all, "rankings_drivers")
merge_and_save(pitstops_all, "pitstops")
merge_and_save(flaps_all, "fastestlaps")
merge_and_save(grid_all, "startinggrid")
merge_and_save(race_rankings_all, "rankings_races")
