"""
Independent validation of the 3 quest answers. Runs OUTSIDE Databricks with plain Python (standard library only):
downloads the live sources and recomputes Q1–Q3, then prints results to compare with the Gold tables.
Shares no code with the pipeline, so agreement means the pipeline logic is right, not just self-consistent.

Usage:  python validation/independent_check.py
"""
import json
import statistics
import urllib.request
from collections import defaultdict
from decimal import Decimal

UA = {"User-Agent": "ubaidu-data-quest (ubaidurahman15@gmail.com)"}  # BLS requires contact info
BLS = "https://download.bls.gov/pub/time.series/pr/"
POP = ("https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
       "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population")


def get(url: str) -> str:
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=60) as r:
        return r.read().decode("utf-8")


# ---- Load sources ----
population = {int(r["Year"]): int(r["Population"]) for r in json.loads(get(POP))["data"]}

rows = []  # (series_id, year, period, Decimal value)
lines = get(BLS + "pr.data.1.AllData").splitlines()
for line in lines[1:]:
    parts = [p.strip() for p in line.split("\t")]
    if len(parts) >= 4 and parts[0]:
        rows.append((parts[0], int(parts[1]), parts[2], Decimal(parts[3])))

titles = {}
for line in get(BLS + "pr.series").splitlines()[1:]:
    parts = [p.strip() for p in line.split("\t")]
    if parts and parts[0]:
        titles[parts[0]] = parts[8]

# ---- Q1: mean + std dev 2013–2018 ----
vals = [population[y] for y in range(2013, 2019)]
print("Q1  years:", len(vals),
      "| mean:", round(statistics.mean(vals), 2),
      "| stdev sample:", round(statistics.stdev(vals), 2),
      "| stdev population:", round(statistics.pstdev(vals), 2))

# ---- Q2: best year per series (hybrid Q05 rule, tie -> earliest year) ----
has_quarterly = {s for s, _, p, _ in rows if p != "Q05"}
totals = defaultdict(Decimal)
counts = defaultdict(int)
for s, y, p, v in rows:
    if (s in has_quarterly and p != "Q05") or (s not in has_quarterly and p == "Q05"):
        totals[(s, y)] += v
        counts[(s, y)] += 1
best = {}
for (s, y), t in totals.items():
    if s not in best or t > best[s][1] or (t == best[s][1] and y < best[s][0]):
        best[s] = (y, t)
print(f"Q2  series: {len(best)} (annual-only: {len(set(best) - has_quarterly)})")
for s in ["PRS30006011", "PRS30006032", "PRS30006081", "PRS85006092"]:
    y, t = best[s]
    print(f"    {s}  best_year={y}  sum={t}  periods={counts[(s, y)]}  {titles.get(s, '')}")

# ---- Q3: PRS30006032 Q01 by year with population where available ----
q3 = sorted((y, v, population.get(y)) for s, y, p, v in rows if s == "PRS30006032" and p == "Q01")
print(f"Q3  rows: {len(q3)}  with population: {sum(1 for _, _, pop in q3 if pop)}")
for y, v, pop in q3:
    if 2012 <= y <= 2021:
        print(f"    {y}  value={v}  population={pop}")
