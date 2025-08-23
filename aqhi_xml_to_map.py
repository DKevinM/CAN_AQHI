#!/usr/bin/env python3
"""
AQHI XML → CSV + Map

Usage examples:
  python aqhi_xml_to_map.py --out-csv aqhi_points.csv --out-html aqhi_map.html
  python aqhi_xml_to_map.py --regions atl ont pnr pyr que
  python aqhi_xml_to_map.py --limit 500
  python aqhi_xml_to_map.py --bbox -121 48 -108 61
"""

import argparse
import re
import sys
from datetime import datetime, timezone
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

def parse_master_list(xml_bytes):
    """Parse master XML index to extract URLs ending in .xml"""
    try:
        root = ET.fromstring(xml_bytes)
        urls = []
        for elem in root.iter():
            text = (elem.text or "").strip()
            if text.startswith("http") and text.lower().endswith(".xml"):
                urls.append(text)
        return urls
    except Exception:
        text = xml_bytes.decode("utf-8", errors="ignore")
        return re.findall(r"https?://[^\\s\"']+\\.xml", text, flags=re.IGNORECASE)

def filter_realtime_observation_urls(urls, regions=None, limit=None):
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
    kept.sort()
    if limit:
        kept = kept[-int(limit):]
    return kept

def fetch_xml(url):
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content

def parse_aqhi_xml(xml_bytes):
    """Extract community name, lat/lon, AQHI, timestamp from a single file"""
    rows = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return rows

    for node in root.iter():
        tag = node.tag.lower().split('}')[-1]
        if any(ct.tag.lower().endswith("latitude") or ct.tag.lower().endswith("longitude")
               or ct.tag.lower().endswith("air_quality_health_index") for ct in node):
            rec = {"name": None, "lat": None, "lon": None, "aqhi": None, "observed": None}
            for child in node:
                ctag = child.tag.lower().split('}')[-1]
                text = (child.text or "").strip()
                if ctag in ("name","community","location_name_en"):
                    rec["name"] = text
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
                    rec["observed"] = text
            if rec["lat"] and rec["lon"]:
                rows.append(rec)
    return rows

def build_map(df, out_html):
    if folium is None:
        print("folium not installed; skipping map")
        return
    if df.empty:
        print("No data to plot")
        return
    m = folium.Map(location=[df["lat"].median(), df["lon"].median()], zoom_start=5)
    for _, r in df.iterrows():
        popup = f"<b>{r['name']}</b><br>AQHI: {r['aqhi']}<br>{r['observed']}"
        folium.CircleMarker([r["lat"], r["lon"]], radius=6,
                            color=r["color"], fill=True, fill_color=r["color"],
                            fill_opacity=0.9, popup=popup).add_to(m)
    m.save(out_html)
    print(f"Wrote {out_html}")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--regions", nargs="*", default=None)
    ap.add_argument("--limit", type=int, default=300)
    ap.add_argument("--bbox", nargs=4, type=float, default=None, help="W S E N")
    ap.add_argument("--out-csv", default="aqhi_points.csv")
    ap.add_argument("--out-html", default="aqhi_map.html")
    ap.add_argument("--out-geojson", default="aqhi_points.geojson")
    args = ap.parse_args()

    xml_list = fetch_xml(MASTER_LIST_URL)
    urls = parse_master_list(xml_list)
    urls = filter_realtime_observation_urls(urls, args.regions, args.limit)

    rows = []
    iterator = tqdm(urls, desc="Downloading") if TQDM else urls
    for u in iterator:
        try:
            xb = fetch_xml(u)
            for rec in parse_aqhi_xml(xb):
                rec["source_file"] = u
                rows.append(rec)
        except Exception as e:
            print(f"Failed {u}: {e}", file=sys.stderr)

    df = pd.DataFrame(rows).dropna(subset=["lat","lon"])
    if "observed" in df:
        df = df.sort_values("observed").groupby(["name","lat","lon"]).tail(1)
    df["color"] = df["aqhi"].apply(aqhi_to_color)

    if args.bbox:
        W,S,E,N = args.bbox
        df = df[(df["lon"]>=W)&(df["lon"]<=E)&(df["lat"]>=S)&(df["lat"]<=N)]

  if args.out_geojson:
      import geopandas as gpd
      gdf = gpd.GeoDataFrame(df,
          geometry=gpd.points_from_xy(df.lon, df.lat),
          crs="EPSG:4326"
      )
      gdf.to_file(args.out_geojson, driver="GeoJSON")
      print(f"Wrote {args.out_geojson}")
  
  # Build map if folium installed
  build_map(df, args.out_html)

if __name__ == "__main__":
    main()
