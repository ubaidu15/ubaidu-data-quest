# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Explore the sources (read-only)
# MAGIC Looks at the BLS folder and the population API before building ingestion. Writes nothing.

# COMMAND ----------

import re
import requests

# BLS blocks requests without contact info (403). This User-Agent follows their policy: bls.gov/bls/pss.htm
HEADERS = {"User-Agent": "ubaidu-data-quest (ubaidurahman15@gmail.com)"}
BLS_URL = "https://download.bls.gov/pub/time.series/pr/"
POP_URL = ("https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
           "?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population")

# COMMAND ----------

# The listing page shows date, time, size and filename for each file. Parse it, don't hardcode names.
html = requests.get(BLS_URL, headers=HEADERS, timeout=30).text
pattern = r"(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2} [AP]M)\s+(\d+)\s+<A HREF=\"([^\"]+)\">"
listing = [{"date": d, "time": t, "size": int(s), "file": h.rsplit("/", 1)[-1]}
           for d, t, s, h in re.findall(pattern, html)]
display(listing)

# COMMAND ----------

# HEAD = headers only (no download). Last-Modified and ETag tell us whether a file has changed.
r = requests.head(BLS_URL + "pr.data.0.Current", headers=HEADERS, timeout=30)
print(r.status_code, r.headers.get("Last-Modified"), r.headers.get("ETag"), r.headers.get("Content-Length"))

# COMMAND ----------

# BLS's own documentation. Read it! It explains codes, Q05 = annual average, and the file formats.
print(requests.get(BLS_URL + "pr.txt", headers=HEADERS, timeout=30).text)

# COMMAND ----------

# Peek at the first lines of every file. Note: tab-separated, padded whitespace in values and headers.
for f in listing:
    text = requests.get(BLS_URL + f["file"], headers=HEADERS, timeout=30).text
    print(f"=== {f['file']} ===")
    print("\n".join(text.splitlines()[:5]), "\n")

# COMMAND ----------

# Population API: JSON with records under "data". Note 2020 is missing (no ACS 1-year release that year).
pop = requests.get(POP_URL, timeout=30)
print(pop.headers.get("ETag"), pop.headers.get("Last-Modified"))  # both None, so we must hash content instead
display(pop.json()["data"])
