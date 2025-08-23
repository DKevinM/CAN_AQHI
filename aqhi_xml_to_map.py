#!/usr/bin/env python3
"""
AQHI XML → CSV + GeoJSON (+ optional Folium HTML map)

Usage examples:
  python aqhi_xml_to_map.py --out-csv data/aqhi_points.csv --out-geojson data/aqhi_points.geojson --out-html data/aqhi_map.html
  python aqhi_xml_to_map.py --regions atl ont pnr pyr que
  python aqhi_xml_to_map.py --limit 400
  python aqhi_xml_to_map.py --bbox -121 48 -108 61   # W S E N

Notes:
- Depends only on: requests, pandas (folium is optional for HTML map).
- Designed to run in GitHub Actions on a schedule.
"""

import argparse
import re
import sys
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
import requests
from xml.etree import ElementTree as ET

try:
    from tqdm import tqdm
    TQDM = True
except Exception:
    TQDM = False

try:
    import folium
except ImportError:
    folium = None

MASTER_LIST_URL = "https://dd.weather.gc.ca/air_quality/doc/AQHI_XML_File_List.xml"

# 11-color AQHI palette (1..10, 10+)
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

def fetch_xml(url: str) -> bytes:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content

def parse_master_list(xml_bytes: bytes) -> List[str]:
    """
    Extract absolute URLs ending in .xml from the master index.
    Works with both normal XML nodes and raw text via regex fallback.
    """
    urls = []
    try:
        root = ET.fromstring(xml_bytes)
        for elem in root.iter():
            text = (elem.text or "").strip()
            if text.startswith("http") and text.lower().endswith(".xml"):
                urls.append(text)
    except Exception:
        pass
    if not urls:
        text = xml_bytes.decode("utf-8", errors="ignore")
        urls = re.findall(r"https?://[^\s\"']+\.xml", text, flags=re.IGNORECASE)

    # Dedup (preserve order)
    out, seen = [], set()
    for u in urls:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out

def filter_realtime_observation_urls(urls: List[str], regions=None, limit: int = 300) -> List[str]:
    regions = set(r.lower() for r in regions) if regions else None
    kept = []
    for u in urls:
        lu = u.lower()
        if "observation" in lu and "realtime" in lu and lu.endswith(".xml"):
            if regions:
                if any(f"/{r}/" in lu for r in regions):
                    kept.append(u)
            else:
                kept.append(u)
    kept.sort()  # filenames usually contain timestamps → sorted ≈ chronological
    if limit:
        kept = kept[-int(limit):]
    return kept

def parse_aqhi_xml(xml_bytes: bytes) -> List[Dict[str, Any]]:
    """
    Extract name, (lat,lon), AQHI, observed time from a single XML file.
    The structure can vary, so we scan heuristically for elements that carry
    latitude/longitude and the air_quality_health_index.
    """
    rows = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return rows

    for node in root.iter():
        # look for a container node that seems to include geo + index fields
        child_tags = [c.tag.lower().split('}')[-1] for c in node]
        if any(t in child_tags for t in ("latitude","lat")) and any(t in child_tags for t in ("longitude","lon")):
            if any(t in child_tags for t in ("air_quality_health_index","aqhi","index")):
                rec = {"name": None, "lat": None, "lon": None, "aqhi": None, "observed": None}
                for child in node:
                    ctag = child.tag.lower().split('}')[-1]
                    text = (child.text or "").strip()
                    if ctag in ("name","community","location_name_en"):
                        rec["name"] = text or rec["name"]
                    elif ctag in ("latitude","lat"):
                        try: rec["lat"] = float(text)
                        except: pass
                    elif ctag in ("longitude","lon"):
                        try: rec["lon"] = float(text)
                        except: pass
                    elif ctag in ("air_quality_health_index","aqhi","index"):
                        try: rec["aqhi"] = float(text)
                        except: pass
                    elif ctag in ("observation_datetime","datetime","date","time"):
                        rec["observed"] = text or rec["observed"]
                if rec["lat"] is not None and rec["lon"] is not None:
                    rows.append(rec)
    return rows

def build_geojson(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Minimal GeoJSON FeatureCollection from a pandas DataFrame with
    lon, lat, and desired properties.
    """
    features = []
    props_cols = [c for c in df.columns if c not in ("lat","lon")]
    for _, r in df.iterrows():
        props = {k: (None if pd.isna(r[k]) else r[k]) for k in props_cols}
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [float(r["lon"]), float(r["lat"])]},
            "properties": props
        })
    return {"type": "FeatureCollection", "features": features}

def save_geojson(obj: Dict[str, Any], path: Path):
    import json
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)

def build_map(df: pd.DataFrame, out_html: Path):
    if folium is None:
        print("folium not installed; skipping HTML map.")
        return
    if df.empty:
        print("No data to plot; skipping HTML map.")
        return
    out_html.parent.mkdir(parents=True, exist_ok=True)

    m = folium.Map(location=[float(df["lat"].median()), float(df["lon"].median())], zoom_start=5, tiles="OpenStreetMap")
    for _, r in df.iterrows():
        popup = f"<b>{r.get('name','(unknown)')}</b><br>AQHI: {r.get('aqhi')}<br>{r.get('observed')}"
        color = r.get("color", "#9e9e9e")
        folium.CircleMarker(
            [float(r["lat"]), float(r["lon"])],
            radius=6, color=color, weight=1,
            fill=True, fill_color=color, fill_opacity=0.9,
            popup=popup
        ).add_to(m)

    # quick legend
    legend = """
    <div style="position: fixed; bottom: 20px; left: 20px; z-index: 9999; background: white; padding: 8px; border: 1px solid #555;">
      <b>AQHI</b><br>
      <div style="display:flex; gap:6px; margin-top:4px;">{swatches}</div>
      <div style="margin-top:4px; font-size: 10px;">1..10, 10+; grey = missing</div>
    </div>
    """.format(swatches="".join(
        f'<span style="display:inline-block;width:14px;height:14px;background:{c};border:1px solid #333"></span>'
        for c in AQHI_COLORS
    ))
    m.get_root().html.add_child(folium.Element(legend))
    m.save(str(out_html))
    print(f"Wrote {out_html}")

def main():
    ap = argparse.ArgumentParser(description="Fetch AQHI realtime XMLs → CSV + GeoJSON (+ optional HTML map)")
    ap.add_argument("--master-url", default=MASTER_LIST_URL, help="Master index XML url")
    ap.add_argument("--regions", nargs="*", default=None, help="Filter by region: atl ont pnr pyr que")
    ap.add_argument("--limit", type=int, default=300, help="Max XML files to parse (newest)")
    ap.add_argument("--bbox", nargs=4, type=float, default=None, help="W S E N filter")
    ap.add_argument("--out-csv", default="data/aqhi_points.csv")
    ap.add_argument("--out-geojson", default="data/aqhi_points.geojson")
    ap.add_argument("--out-html", default="data/aqhi_map.html")
    args = ap.parse_args()

    # 1) Master list → candidate XML URLs
    xml_list = fetch_xml(args.master_url)
    urls = parse_master_list(xml_list)
    urls = filter_realtime_observation_urls(urls, regions=args.regions, limit=args.limit)
    if not urls:
        print("No realtime observation XML URLs found.", file=sys.stderr)
        sys.exit(1)

    # 2) Fetch & parse each XML
    rows = []
    iterator = tqdm(urls, desc="Downloading XMLs") if TQDM else urls
    for u in iterator:
        try:
            xb = fetch_xml(u)
            for rec in parse_aqhi_xml(xb):
                rec["source_file"] = u
                rows.append(rec)
        except Exception as e:
            print(f"Warning: failed {u}: {e}", file=sys.stderr)

    if not rows:
        print("No observation records parsed.", file=sys.stderr)
        sys.exit(1)

    # 3) Tidy dataframe
    df = pd.DataFrame(rows).dropna(subset=["lat","lon"])
    if "observed" in df.columns:
        df = df.sort_values("observed").groupby(["name","lat","lon"], as_index=False).tail(1)
    df["color"] = df["aqhi"].apply(aqhi_to_color)

    # 4) Optional bbox filter
    if args.bbox:
        W, S, E, N = args.bbox
        df = df[(df["lon"] >= W) & (df["lon"] <= E) & (df["lat"] >= S) & (df["lat"] <= N)]

    # 5) Outputs
    out_csv = Path(args.out_csv); out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    print(f"Wrote {out_csv} ({len(df)} rows)")

    # GeoJSON FeatureCollection (no GeoPandas needed)
    geojson = build_geojson(df[["name","aqhi","observed","color","lat","lon"]])
    out_geo = Path(args.out_geojson)
    save_geojson(geojson, out_geo)
    print(f"Wrote {out_geo}")

    # Optional Folium map
    if args.out_html:
        build_map(df, Path(args.out_html))

if __name__ == "__main__":
    main()
