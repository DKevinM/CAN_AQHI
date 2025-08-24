#!/usr/bin/env python3
"""
GeoMet AQHI (observations + forecasts) → CSV + GeoJSON (+ optional Folium HTML)

Usage:
  python scripts/aqhi_geomet_all.py \
    --out-dir data \
    --html data/index.html

Optional filters:
  python scripts/aqhi_geomet_all.py --bbox -121 48 -108 61
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

OBS_API = "https://api.weather.gc.ca/collections/aqhi-observations-realtime/items"
FCST_API = "https://api.weather.gc.ca/collections/aqhi-forecasts-realtime/items"

# 11-color AQHI palette (1..10, 10+); grey for missing
AQHI_COLORS = [
    "#01cbff", "#0099cb", "#016797", "#fffe03", "#ffcb00", "#ff9835",
    "#fd6866", "#fe0002", "#cc0001", "#9a0100", "#640100"
]
MISSING_COLOR = "#9e9e9e"

def aqhi_to_color(val):
    if val is None or pd.isna(val):
        return MISSING_COLOR
    try:
        v = float(val)
    except Exception:
        return MISSING_COLOR
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

def fetch_all_items(api_url: str, bbox: Optional[List[float]] = None) -> List[Dict[str, Any]]:
    """Generic pager for GeoMet OGC API collections."""
    items = []
    params = {"f": "json", "limit": 1000}
    if bbox:
        params["bbox"] = ",".join(map(str, bbox))
    url = api_url
    while url:
        r = requests.get(url, params=params if url == api_url else None, timeout=60)
        r.raise_for_status()
        data = r.json()
        items.extend(data.get("features", []))
        # find next link (if any)
        next_url = None
        for link in data.get("links", []):
            if link.get("rel") == "next" and link.get("href"):
                next_url = link["href"]
                break
        url = next_url
        params = None
    return items

def obs_to_df(features: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for f in features:
        p = f.get("properties", {})
        g = f.get("geometry", {}) or {}
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
    if not df.empty and "observed" in df.columns:
        df = df.sort_values("observed").groupby("id", as_index=False).tail(1)
    df["color"] = df["aqhi"].apply(aqhi_to_color)
    return df

def fcst_to_df(features: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for f in features:
        p = f.get("properties", {})
        g = f.get("geometry", {}) or {}
        coords = g.get("coordinates") or [None, None]
        fp = p.get("forecast_period", {}) or {}
        def getp(n):
            d = fp.get(f"period_{n}", {}) or {}
            return d.get("aqhi"), d.get("forecast_period_en") or d.get("forecast_period_fr")
        p1_aqhi, p1_label = getp(1)
        p2_aqhi, p2_label = getp(2)
        p3_aqhi, p3_label = getp(3)
        p4_aqhi, p4_label = getp(4)
        p5_aqhi, p5_label = getp(5)
        rows.append({
            "id": p.get("id") or p.get("location_id"),
            "name": p.get("location_name_en") or p.get("location_name_fr"),
            "province": p.get("province"),
            "forecast_datetime": p.get("forecast_datetime"),
            "publication_datetime": p.get("publication_datetime"),
            "p1_label": p1_label,
            "p1_aqhi": p1_aqhi,
            "p2_label": p2_label,
            "p2_aqhi": p2_aqhi,
            "p3_label": p3_label,
            "p3_aqhi": p3_aqhi,
            "p4_label": p4_label,
            "p4_aqhi": p4_aqhi,
            "p5_label": p5_label,
            "p5_aqhi": p5_aqhi,
            "lon": coords[0],
            "lat": coords[1],
        })
    df = pd.DataFrame(rows).dropna(subset=["lat","lon"])
    if not df.empty and "forecast_datetime" in df.columns:
        df = df.sort_values("forecast_datetime").groupby("id", as_index=False).tail(1)
    # color first forecast period
    df["p1_color"] = df["p1_aqhi"].apply(aqhi_to_color)
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

def build_map(obs_df: pd.DataFrame, fcst_df: pd.DataFrame, out_html: Path):
    if folium is None:
        print("folium not installed; skipping HTML map.")
        return
    if obs_df.empty and fcst_df.empty:
        print("No data to map; skipping HTML map.")
        return

    out_html.parent.mkdir(parents=True, exist_ok=True)
    # pick a center
    base_df = obs_df if not obs_df.empty else fcst_df
    m = folium.Map(location=[float(base_df["lat"].median()), float(base_df["lon"].median())],
                   zoom_start=5, tiles="OpenStreetMap")

    # Observations layer
    obs_layer = None
    if not obs_df.empty:
        def _obspt(r):
            color = r.get("color", MISSING_COLOR)
            return dict(radius=6, color=color, weight=1, fill=True, fillColor=color, fillOpacity=0.9)
        obs_layer = folium.FeatureGroup(name="AQHI (observed)", show=True)
        for _, r in obs_df.iterrows():
            folium.CircleMarker(
                [float(r["lat"]), float(r["lon"])],
                popup=f"<b>{r.get('name','(unknown)')}</b><br>AQHI: {r.get('aqhi','—')}<br>{r.get('observed','')}",
                **_obspt(r)
            ).add_to(obs_layer)
        obs_layer.add_to(m)

    # Forecast layer (use p1)
    fcst_layer = None
    if not fcst_df.empty:
        def _fcstpt(r):
            color = r.get("p1_color", MISSING_COLOR)
            return dict(radius=6, color=color, weight=1, fill=True, fillColor=color, fillOpacity=0.9)
        fcst_layer = folium.FeatureGroup(name="AQHI (forecast: next period)", show=False)
        for _, r in fcst_df.iterrows():
            label = r.get("p1_label") or "Next period"
            folium.CircleMarker(
                [float(r["lat"]), float(r["lon"])],
                popup=(f"<b>{r.get('name','(unknown)')}</b><br>"
                       f"{label}: {r.get('p1_aqhi','—')}<br>"
                       f"Issued: {r.get('publication_datetime','')}"),
                **_fcstpt(r)
            ).add_to(fcst_layer)
        fcst_layer.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)

    # quick palette legend
    legend = """
    <div style="position: fixed; bottom: 20px; left: 20px; z-index: 9999; background: white; padding: 8px; border: 1px solid #555;">
      <b>AQHI palette</b><br>
      <div style="display:flex; gap:6px; margin-top:4px;">{sw}</div>
      <div style="margin-top:4px; font-size: 10px;">1..10, 10+; grey = missing</div>
    </div>
    """.format(sw="".join(
        f'<span style="display:inline-block;width:14px;height:14px;background:{c};border:1px solid #333"></span>'
        for c in AQHI_COLORS
    ))
    m.get_root().html.add_child(folium.Element(legend))
    m.save(str(out_html))
    print(f"Wrote {out_html}")

def main():
    ap = argparse.ArgumentParser(description="GeoMet AQHI (obs+forecast) → CSV + GeoJSON (+ optional HTML)")
    ap.add_argument("--bbox", nargs=4, type=float, default=None, help="W S E N (optional)")
    ap.add_argument("--out-dir", default="data", help="Output directory")
    ap.add_argument("--html", default=None, help="Optional HTML map output path, e.g., data/index.html")
    args = ap.parse_args()

    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)

    # Fetch
    obs_feats = fetch_all_items(OBS_API, bbox=args.bbox)
    fcst_feats = fetch_all_items(FCST_API, bbox=args.bbox)

    # DataFrames
    obs_df = obs_to_df(obs_feats)
    fcst_df = fcst_to_df(fcst_feats)

    # Optional extra bbox filter (if not used upstream)
    if args.bbox:
        W,S,E,N = args.bbox
        obs_df = obs_df[(obs_df["lon"]>=W)&(obs_df["lon"]<=E)&(obs_df["lat"]>=S)&(obs_df["lat"]<=N)]
        fcst_df = fcst_df[(fcst_df["lon"]>=W)&(fcst_df["lon"]<=E)&(fcst_df["lat"]>=S)&(fcst_df["lat"]<=N)]

    # Write CSV
    obs_csv = out_dir / "aqhi_observations.csv"
    fcst_csv = out_dir / "aqhi_forecasts.csv"
    obs_df.to_csv(obs_csv, index=False)
    fcst_df.to_csv(fcst_csv, index=False)
    print(f"Wrote {obs_csv} ({len(obs_df)} rows)")
    print(f"Wrote {fcst_csv} ({len(fcst_df)} rows)")

    # Write GeoJSON
    obs_geo = out_dir / "aqhi_observations.geojson"
    fcst_geo = out_dir / "aqhi_forecasts.geojson"
    save_geojson(df_to_geojson(obs_df[["id","name","province","aqhi","observed","color","lat","lon"]]), obs_geo)
    save_geojson(df_to_geojson(fcst_df[["id","name","province","forecast_datetime","publication_datetime",
                                        "p1_label","p1_aqhi","p2_label","p2_aqhi","p3_label","p3_aqhi",
                                        "p4_label","p4_aqhi","p5_label","p5_aqhi","p1_color","lat","lon"]]), fcst_geo)
    print(f"Wrote {obs_geo}")
    print(f"Wrote {fcst_geo}")

    # Optional HTML map
    if args.html:
        build_map(obs_df, fcst_df, Path(args.html))

if __name__ == "__main__":
    main()
