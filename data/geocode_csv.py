import os, time, json, csv, pandas as pd
from geopy.geocoders import GoogleV3

# === CONFIG ===
INPUT = "blitz_first_night.csv"   # your CSV file
OUTPUT_CSV = "blitz_first_night_geocoded.csv"
OUTPUT_GEO = "blitz_first_night.geojson"
CACHE_FILE = "geocode_cache.csv"

API_KEY = "AIzaSyCeIF5N_K44rplh-_FM6OXinHx0LOPlWog"

# --- Normalizer for address text ---
REPLACEMENTS = {
    " Rd ": " Road ",
    " St ": " Street ",
    " Pl ": " Place ",
    " Sq ": " Square ",
    " Ave ": " Avenue ",
    " Cres ": " Crescent ",
    " Gdns ": " Gardens ",
    " Ter ": " Terrace ",
    " Bldgs ": " Buildings ",
    " Cl ": " Close ",
    " Ln ": " Lane ",
    " Pk ": " Park ",
    " H St ": " High Street ",
}
def normalize_address(s):
    if not isinstance(s, str):
        return s
    s = " " + s.strip() + " "   # pad so replacements work on edges
    for k,v in REPLACEMENTS.items():
        s = s.replace(k, v)
    return s.strip()

# Load CSV
df = pd.read_csv(INPUT)
assert "Location" in df.columns, "CSV must have a 'Location' column"

# Add columns if missing
for c in ["lat","lng","geocoder_match","geocoder_source"]:
    if c not in df.columns:
        df[c] = pd.NA

# Simple cache: query -> (lat,lng,formatted)
cache = {}
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cache[row["query"]] = (row["lat"], row["lng"], row["formatted"])

def save_cache():
    with open(CACHE_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["query","lat","lng","formatted"])
        for q,(lat,lng,fmt) in cache.items():
            w.writerow([q,lat,lng,fmt])

def build_query(loc):
    loc = normalize_address(loc)
    if not loc:
        return None
    return f"{loc}, London, UK"

geolocator = GoogleV3(api_key=API_KEY, timeout=10, domain="maps.googleapis.com")

def geocode_query(q):
    if q in cache:
        return cache[q]
    loc = geolocator.geocode(q, region="uk", components={"country":"GB"})
    if loc:
        cache[q] = (f"{loc.latitude:.8f}", f"{loc.longitude:.8f}", loc.address)
    else:
        cache[q] = ("","","")
    return cache[q]

# Build unique list of addresses
queries = []
for _,row in df.iterrows():
    if pd.notna(row["lat"]) and pd.notna(row["lng"]):
        continue
    q = build_query(row["Location"])
    if q: queries.append(q)
queries = list(dict.fromkeys(queries))

print(f"Unique addresses to geocode: {len(queries)}")

# Run geocoding
for i,q in enumerate(queries, 1):
    lat,lng,fmt = geocode_query(q)
    if i % 25 == 0:
        print(f"  {i}/{len(queries)} done…")
        save_cache()
    time.sleep(0.05)  # gentle pacing

save_cache()

# Write results back into df
for i,row in df.iterrows():
    if pd.notna(row["lat"]) and pd.notna(row["lng"]):
        continue
    q = build_query(row["Location"])
    if not q or q not in cache:
        continue
    lat,lng,fmt = cache[q]
    df.at[i,"lat"] = lat if lat else pd.NA
    df.at[i,"lng"] = lng if lng else pd.NA
    df.at[i,"geocoder_match"] = fmt
    df.at[i,"geocoder_source"] = "Google"

# Save CSV + GeoJSON
df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

features = []
for _,r in df.dropna(subset=["lat","lng"]).iterrows():
    features.append({
        "type":"Feature",
        "geometry":{"type":"Point","coordinates":[float(r["lng"]), float(r["lat"])]},
        "properties": {k: (None if pd.isna(v) else v) for k,v in r.items()}
    })
geo = {"type":"FeatureCollection","features":features}
with open(OUTPUT_GEO, "w", encoding="utf-8") as f:
    json.dump(geo, f)

print(f"Saved: {OUTPUT_CSV}\nSaved: {OUTPUT_GEO}")
