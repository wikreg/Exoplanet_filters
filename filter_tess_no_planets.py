#!/usr/bin/env python3
"""Build a CSV of TESS TICs with light curves and no known exoplanets.

The script retrieves all TESS time series observations from MAST and cross-
checks them against the NASA Exoplanet Archive to exclude targets that are
already associated with confirmed or candidate planets.  The resulting CSV
contains two columns: ``tic_id`` and ``cadence`` (exposure time in seconds).

Example:
    python filter_tess_no_planets.py --output tess_no_planets.csv
"""
from __future__ import annotations

import argparse
from typing import Iterable, Set

import certifi
import pandas as pd
import requests
from astroquery.mast import Observations
import os

os.environ.setdefault("NO_PROXY", "*")
os.environ.setdefault("no_proxy", "*")

EXOPLANET_ARCHIVE_URL = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"


def fetch_confirmed_tic_ids() -> Set[str]:
    """Return a set of TIC IDs that host confirmed or candidate exoplanets."""
    query = (
        "select distinct tic_id from pscomppars where tic_id is not null"
    )
    params = {"query": query, "format": "json", "maxrec": 100000}
    try:
        response = requests.get(
            EXOPLANET_ARCHIVE_URL,
            params=params,
            timeout=60,
            verify=certifi.where(),
            proxies={},
        )
    except requests.exceptions.SSLError:
        # Fall back to skipping certificate verification if necessary.
        response = requests.get(
            EXOPLANET_ARCHIVE_URL,
            params=params,
            timeout=60,
            verify=False,
            proxies={},
        )
    response.raise_for_status()
    data = response.json()
    tic_ids: Set[str] = set()
    for row in data:
        tic = row.get("tic_id")
        if tic:
            tic_ids.add(tic.replace("TIC", "").strip())
    return tic_ids


def fetch_lightcurve_targets(columns: Iterable[str] | None = None) -> pd.DataFrame:
    """Return DataFrame of TIC IDs with available TESS light curves.

    Parameters
    ----------
    columns : Iterable[str], optional
        Columns to request from the MAST observations service.
    """
    if columns is None:
        columns = ["target_name", "t_exptime"]

    # Explicitly configure certificate bundle to avoid SSL errors.
    Observations._session.verify = certifi.where()
    Observations._session.proxies = {}

    try:
        obs_table = Observations.query_criteria(
            project="TESS", dataproduct_type="timeseries", obs_collection="TESS"
        )
    except requests.exceptions.SSLError:
        Observations._session.verify = False
        obs_table = Observations.query_criteria(
            project="TESS", dataproduct_type="timeseries", obs_collection="TESS"
        )
    df = obs_table.to_pandas()[list(columns)].dropna()
    df["tic_id"] = df["target_name"].str.extract(r"TIC\s*(\d+)", expand=False)
    df["cadence"] = df["t_exptime"].astype(float)
    return df[["tic_id", "cadence"]].dropna().drop_duplicates()


def main(output_path: str) -> None:
    confirmed_tics = fetch_confirmed_tic_ids()
    candidates = fetch_lightcurve_targets()
    filtered = candidates[~candidates["tic_id"].isin(confirmed_tics)]
    filtered.to_csv(output_path, index=False)
    print(f"Wrote {len(filtered)} rows to {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate CSV of TESS TICs with light curves and no known planets"
    )
    parser.add_argument(
        "--output",
        default="tess_no_planets.csv",
        help="Output CSV filename",
    )
    args = parser.parse_args()
    main(args.output)