#!/usr/bin/env python3
"""
mclains_mods_update.py

What it does
- Reads your existing mods CSV
- For each mod, finds the latest Minecraft version it supports (e.g., 1.21.5)
- Sets compatibility_flag = "1.21.x" if the mod supports ANY 1.21.* version
- Writes a NEW CSV with columns in your preferred order (compatibility_flag is 4th)

Inputs/Outputs
- Input CSV:  data/mclains_mods_database.csv        (change below if needed)
- Output CSV: data/mclains_mods_database_updated.csv

APIs
- CurseForge (recommended): requires an API key in env var CURSEFORGE_API_KEY
- Modrinth: no key required

Install
  pip install pandas requests
Optional (for .env):
  pip install python-dotenv
"""

from __future__ import annotations

import os
import re
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

# Optional .env support (won't fail if not installed)
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


# -----------------------------
# CONFIG (EDIT THESE)
# -----------------------------
INPUT_CSV_PATH = os.getenv("MODS_DB_INPUT", "../data/mclains_mods_database_v3.csv")
OUTPUT_CSV_PATH = os.getenv("MODS_DB_OUTPUT", "../data/mclains_mods_database_updated.csv")

# If you want to treat "1.21.x" as the threshold for the red tag:
FLAG_MAJOR_MINOR = (1, 21)

# Slow down requests a bit to be polite
SLEEP_BETWEEN_MODS_SEC = 0.35

# Prefer CurseForge first if API key is present
CURSEFORGE_API_KEY = os.getenv("CURSEFORGE_API_KEY", "").strip()

CF_BASE = "https://api.curseforge.com/v1"
MR_BASE = "https://api.modrinth.com/v2"

CF_HEADERS = {
    "Accept": "application/json",
    "x-api-key": CURSEFORGE_API_KEY,
}


# -----------------------------
# COLUMN ORDER (as requested)
# compatibility_flag is 4th
# -----------------------------
COLUMN_ORDER = [
    "mod name",
    "mc_version",
    "latest_version_available",
    "compatibility_flag",
    "mod version number",
    "author",
    "project id",
    "file id",
    "source",
    "url",
    "loader",
    "dependencies",
    "required",
]


# -----------------------------
# Helpers: version parsing
# -----------------------------
SEMVER_RE = re.compile(r"^(\d+)\.(\d+)(?:\.(\d+))?$")

def parse_mc_version(v: str) -> Optional[Tuple[int, int, int]]:
    """
    Parse Minecraft versions like:
      "1.21" -> (1, 21, 0)
      "1.21.5" -> (1, 21, 5)
    Ignore things like "Forge", "Fabric", "Java 17", "Snapshot", etc.
    """
    v = (v or "").strip()
    m = SEMVER_RE.match(v)
    if not m:
        return None
    major = int(m.group(1))
    minor = int(m.group(2))
    patch = int(m.group(3) or 0)
    return (major, minor, patch)

def max_mc_version(versions: List[str]) -> Optional[str]:
    """
    Return the highest MC version string from a list, comparing semver tuples.
    """
    best_tup = None
    best_str = None
    for v in versions:
        t = parse_mc_version(v)
        if not t:
            continue
        if best_tup is None or t > best_tup:
            best_tup = t
            best_str = v
    return best_str

def supports_flag(v: Optional[str]) -> bool:
    """
    True if the MC version is >= FLAG_MAJOR_MINOR (e.g., >= 1.21.0)
    """
    if not v:
        return False
    t = parse_mc_version(v)
    if not t:
        return False
    return (t[0], t[1]) >= FLAG_MAJOR_MINOR


# -----------------------------
# API clients
# -----------------------------
@dataclass
class MatchResult:
    source: str          # "CurseForge" or "Modrinth"
    project_id: str
    url: Optional[str] = None
    author: Optional[str] = None


def curseforge_enabled() -> bool:
    return bool(CURSEFORGE_API_KEY)


def cf_search_mod(name: str) -> Optional[Dict[str, Any]]:
    """
    Search CurseForge mods for Minecraft.
    Returns the best hit (naive: first result).
    """
    params = {
        "gameId": 432,          # Minecraft
        "searchFilter": name,
        "pageSize": 5,
    }
    r = requests.get(f"{CF_BASE}/mods/search", headers=CF_HEADERS, params=params, timeout=20)
    r.raise_for_status()
    data = r.json().get("data") or []
    return data[0] if data else None


def cf_get_files(mod_id: int, page_size: int = 50) -> List[Dict[str, Any]]:
    """
    Fetch up to `page_size` latest files (first page) for a mod.
    """
    params = {
        "pageSize": page_size,
        # sortField: 3 = FileDate (commonly), sortOrder: 2 = Desc
        "sortField": 3,
        "sortOrder": 2,
    }
    r = requests.get(f"{CF_BASE}/mods/{mod_id}/files", headers=CF_HEADERS, params=params, timeout=25)
    r.raise_for_status()
    return r.json().get("data") or []


def mr_search_mod(name: str) -> Optional[Dict[str, Any]]:
    """
    Search Modrinth projects for mods.
    """
    params = {
        "query": name,
        "limit": 5,
        "facets": json.dumps([["project_type:mod"]]),
    }
    r = requests.get(f"{MR_BASE}/search", params=params, timeout=20)
    r.raise_for_status()
    hits = r.json().get("hits") or []
    return hits[0] if hits else None


def mr_get_versions(project_id: str) -> List[Dict[str, Any]]:
    r = requests.get(f"{MR_BASE}/project/{project_id}/version", timeout=25)
    r.raise_for_status()
    return r.json() or []


# -----------------------------
# Matching + latest MC version discovery
# -----------------------------
def find_project_if_missing(mod_name: str) -> Optional[MatchResult]:
    """
    Prefer CurseForge (if enabled). Fall back to Modrinth.
    """
    # CurseForge first
    if curseforge_enabled():
        try:
            hit = cf_search_mod(mod_name)
            if hit:
                links = hit.get("links") or {}
                authors = hit.get("authors") or []
                author = authors[0].get("name") if authors else None
                return MatchResult(
                    source="CurseForge",
                    project_id=str(hit["id"]),
                    url=links.get("websiteUrl"),
                    author=author,
                )
        except Exception:
            pass

    # Modrinth fallback
    try:
        hit = mr_search_mod(mod_name)
        if hit:
            slug = hit.get("slug")
            url = f"https://modrinth.com/mod/{slug}" if slug else None
            return MatchResult(
                source="Modrinth",
                project_id=str(hit["project_id"]),
                url=url,
                author=hit.get("author"),
            )
    except Exception:
        pass

    return None


def get_latest_mc_and_file_id(source: str, project_id: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns:
      latest_mc_version_available (e.g., "1.21.5")
      file_id (best file/version id that corresponds to that latest MC support)
    """
    if source == "CurseForge" and curseforge_enabled():
        files = cf_get_files(int(project_id), page_size=80)

        # For each file, compute max MC version it supports; pick file with the highest MC version,
        # then (tie-break) most recent fileDate since the list is already sorted desc by fileDate.
        best_mc_tup = None
        best_mc_str = None
        best_file_id = None

        for f in files:
            gvs = f.get("gameVersions") or []
            mc = max_mc_version([v for v in gvs if parse_mc_version(v)])
            if not mc:
                continue
            t = parse_mc_version(mc)
            if not t:
                continue

            if best_mc_tup is None or t > best_mc_tup:
                best_mc_tup = t
                best_mc_str = mc
                best_file_id = str(f.get("id"))

        return best_mc_str, best_file_id

    if source == "Modrinth":
        versions = mr_get_versions(project_id)

        best_mc_tup = None
        best_mc_str = None
        best_version_id = None

        for v in versions:
            gvs = v.get("game_versions") or []
            mc = max_mc_version([x for x in gvs if parse_mc_version(x)])
            if not mc:
                continue
            t = parse_mc_version(mc)
            if not t:
                continue

            if best_mc_tup is None or t > best_mc_tup:
                best_mc_tup = t
                best_mc_str = mc
                best_version_id = str(v.get("id"))

        return best_mc_str, best_version_id

    return None, None


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    df = pd.read_csv(INPUT_CSV_PATH)

    # Ensure required columns exist (names as in your original schema)
    # Note: order is handled later.
    required_cols = set(COLUMN_ORDER)
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    # Normalize types
    df["project id"] = df["project id"].astype("string")
    df["file id"] = df["file id"].astype("string")
    df["source"] = df["source"].astype("string")

    # Make sure these columns are treated as text so we can safely write "1.20.1", etc.
    if "latest_version_available" not in df.columns:
        df["latest_version_available"] = pd.Series(dtype="string")
    else:
        df["latest_version_available"] = df["latest_version_available"].astype("string")

    if "compatibility_flag" not in df.columns:
        df["compatibility_flag"] = pd.Series(dtype="string")
    else:
        df["compatibility_flag"] = df["compatibility_flag"].astype("string")


    for idx, row in df.iterrows():
        mod_name = str(row.get("mod name") or "").strip()
        if not mod_name:
            continue

        raw_source = row.get("source")

        if raw_source is None or pd.isna(raw_source):
            source = ""
        else:
            source = str(raw_source).strip()

        raw_project_id = row.get("project id")

        if raw_project_id is None or pd.isna(raw_project_id):
            project_id = ""
        else:
            project_id = str(raw_project_id).strip()

        # 1) Find project id if missing
        if not project_id or project_id.lower() in ("nan", "none", ""):
            match = find_project_if_missing(mod_name)
            if match:
                df.at[idx, "source"] = match.source
                df.at[idx, "project id"] = match.project_id
                source = match.source
                project_id = match.project_id

                # Only fill url/author if blank
                if not str(row.get("url") or "").strip() and match.url:
                    df.at[idx, "url"] = match.url
                if not str(row.get("author") or "").strip() and match.author:
                    df.at[idx, "author"] = match.author

        # 2) If we have a project id + source, compute latest MC version supported + file id
        latest_mc = None
        best_file_id = None
        if project_id and project_id.lower() not in ("nan", "none"):
            try:
                latest_mc, best_file_id = get_latest_mc_and_file_id(source, project_id)
            except Exception:
                latest_mc, best_file_id = None, None

        if latest_mc:
            df.at[idx, "latest_version_available"] = latest_mc

        if best_file_id:
            df.at[idx, "file id"] = best_file_id

        # 3) Compute compatibility flag (1.21.x if latest_mc is 1.21.* or higher)
        flag = "1.21.x" if supports_flag(latest_mc) else None
        df.at[idx, "compatibility_flag"] = flag

        time.sleep(SLEEP_BETWEEN_MODS_SEC)

    # Enforce output column order (compatibility_flag is 4th)
    df = df[COLUMN_ORDER]

    df.to_csv(OUTPUT_CSV_PATH, index=False)
    print(f"✅ Done.\nInput : {INPUT_CSV_PATH}\nOutput: {OUTPUT_CSV_PATH}\n")


if __name__ == "__main__":
    main()
