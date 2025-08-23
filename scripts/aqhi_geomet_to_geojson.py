#!/usr/bin/env python3
"""
GeoMet AQHI → CSV + GeoJSON (+ optional Folium HTML map)

Usage:
  python scripts/aqhi_geomet_to_geojson.py \
    --out-csv data/aqhi_points.csv \
    --out-geojson data/aqhi_points.geojson \
    --out-html data/aqhi_map.html
  # Optional filters
  python scripts/aqhi_geomet_to_geojson.py --bbox -121 48 -108 61
"""

import argparse, json, sys
from pathlib import Path
from typing import Dict, Any, List, Optional

import pandas as pd
import requests

try:
    import folium
except ImportError:
    folium = None

API = "https://api.weather.gc.ca/collections/aqhi-observations-realtime/items"

# Your 11-color AQHI palette (1..10, 10+)
AQHI_COLORS = [
    "#01cbff", "#0099cb", "#016797", "#fffe03", "#ffcb00", "#ff9835",
    "#fd6866", "#fe0002", "#cc0001", "#9a0100", "#640100"
]

def aqhi_to_color(val):
    if val is None or pd.isna(val):
        return "#9e9e9e"
    try:
        v = float(val)
    except Exception:
        return "#9e9e9e"
    if v <= 1:  return AQHI_COLORS[0]
    if v <= 2:  return AQHI_COLORS[1]
    if v <= 3:  return AQHI_COLORS[2]
    if v <= 4:  return AQHI_COLORS[3]
    if v <= 5:  return AQHI_COLORS[4]
    if v <= 6:  return AQHI_COLORS[5]
    if v <= 7:  return AQHI_COLORS[6]
    if v <= 8:  return AQHI_COLORS[7]
    if v <= 9:  return AQHI_COLORS[8]
    if v <= 10: return AQHI_COLORS[9]
    return AQHI_COLORS[10]

def fetch_all_items(bbox: Optional[List[float]] = None) -> List[Dict[str, Any]]:
    """Fetch all items via paging (GeoMet OGC API)."""
    items = []
    params = {"f": "json", "limit": 1000}  # pull big pages
    if bbox:
        # bbox = [W,S,E,N]
        params["bbox"] = ",".join(map(str, bbox))

    url = API
    while url:
        r = requests.get(url, params=params if url == API else None, timeout=60)
        r.raise_for_status()
        data = r.json()
        items.extend(data.get("features", []))
        # find next link
        next_url = None
        for link in data.get("links", []):
            if link.get("rel") == "next" and link.get("href"):
                next_url = link["href"]
                break
        url = next_url
        params = None  # only on first request
    return items

def to_dataframe(features: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for f in features:
        p = f.get("properties", {})
        g = f.get("geometry", {})
        coords = g.get("coordinates") or [None, None]
        rows.append({
            "id": p.get("id") or p.get("location_id"),
            "name": p.get("location_name_en") or p.get("location_name_fr"),
            "province": p.get("province"),
            "aqhi": p.get("aqhi"),
            "observed": p.get("observation_datetime"),
            "lon": coords[0],
            "lat": coords[1],
        })
    df = pd.DataFrame(rows).dropna(subset=["lat","lon"])
    # keep latest per id
    if "observed" in df.columns:
        df = df.sort_values("observed").groupby("id", as_index=False).tail(1)
    df["color"] = df["aqhi"].apply(aqhi_to_color)
    return df

def df_to_geojson(df: pd.DataFrame) -> Dict[str, Any]:
    feats = []
    for _, r in df.iterrows():
        props = {k: (None if pd.isna(r[k]) else r[k]) for k in df.columns if k not in ("lat","lon")}
        feats.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [float(r["lon"]), float(r["lat"])]},
            "properties": props
        })
    return {"type": "FeatureCollection", "features": feats}

def save_geojson(obj: Dict[str, Any], path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)

def build_map(df: pd.DataFrame, out_html: Path):
    if folium is None or df.empty:
        return
    out_html.parent.mkdir(parents=True, exist_ok=True)
    m = folium.Map(location=[float(df["lat"].median()), float(df["lon"].median())], zoom_start=5, tiles="OpenStreetMap")
    for _, r in df.iterrows():
        color = r.get("color", "#9e9e9e")
        popup = f"<b>{r.get('name','(unknown)')}</b><br>AQHI: {r.get('aqhi')}<br>{r.get('observed')}"
        folium.CircleMarker(
            [float(r["lat"]), float(r["lon"])],
            radius=6, color=color, weight=1, fill=True, fill_color=color, fill_opacity=0.9,
            popup=popup
        ).add_to(m)
    # simple legend
    legend = """
    <div style="position: fixed; bottom: 20px; left: 20px; z-index: 9999; background: white; padding: 8px; border: 1px solid #555;">
      <b>AQHI</b><br>
      <div style="display:flex; gap:6px; margin-top:4px;">{sw}</div>
      <div style="margin-top:4px; font-size: 10px;">1..10, 10+; grey = missing</div>
    </div>
    """.format(sw="".join(
        f'<span style="display:inline-block;width:14px;height:14px;background:{c};border:1px solid #333"></span>'
        for c in AQHI_COLORS
    ))
    m.get_root().html.add_child(folium.Element(legend))
    m.save(str(out_html))

def main():
    ap = argparse.ArgumentParser(description="GeoMet AQHI → CSV + GeoJSON (+ optional HTML map)")
    ap.add_argument("--bbox", nargs=4, type=float, default=None, help="W S E N (optional)")
    ap.add_argument("--out-csv", default="data/aqhi_points.csv")
    ap.add_argument("--out-geojson", default="data/aqhi_points.geojson")
    ap.add_argument("--out-html", default="data/aqhi_map.html")
    args = ap.parse_args()

    feats = fetch_all_items(bbox=args.bbox)
    if not feats:
        print("No features returned.", file=sys.stderr)
        sys.exit(1)

    df = to_dataframe(feats)

    # Optional bbox filter (redundant if you used bbox in fetch)
    if args.bbox:
        W,S,E,N = args.bbox
        df = df[(df["lon"]>=W)&(df["lon"]<=E)&(df["lat"]>=S)&(df["lat"]<=N)]

    out_csv = Path(args.out_csv); out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"Wrote {out_csv} ({len(df)} rows)")

    geojson = df_to_geojson(df[["id","name","province","aqhi","observed","color","lat","lon"]])
    out_geo = Path(args.out_geojson)
    save_geojson(geojson, out_geo)
    print(f"Wrote {out_geo}")

    if args.out_html:
        build_map(df, Path(args.out_html))
        print(f"Wrote {args.out_html}")

if __name__ == "__main__":
    main()
