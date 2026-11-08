"""
Builds the Databricks AI/BI (Lakeview) dashboard definition for the Data Quest Gold layer.
Output: dashboards/data_quest_dashboard.lvdash.json (importable in Databricks, or sent via the Lakeview API).

Audience: non-technical stakeholders. Plain-English titles, filters, short explanations, and Gold tables only
(plus the ops health view for "data as of").
Usage: python dashboards/build_dashboard.py
"""
import json
from pathlib import Path

S = "workspace.data_quest"


# ---------- helpers that emit Lakeview widget JSON ----------
def text(name, lines, x, y, w, h):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": lines}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def counter(name, ds, field, title, x, y, w=2, h=3):
    return {"widget": {"name": name,
                       "queries": [{"name": "main_query", "query": {
                           "datasetName": ds, "fields": [{"name": field, "expression": f"`{field}`"}],
                           "disaggregated": True}}],
                       "spec": {"version": 2, "widgetType": "counter",
                                "encodings": {"value": {"fieldName": field, "displayName": title}},
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def chart(name, kind, ds, xf, yf, title, xlab, ylab, x, y, w, h, agg_count=False, x_scale="categorical"):
    """Line or bar chart. agg_count=True plots COUNT(yf) grouped by xf."""
    y_name = f"count({yf})" if agg_count else yf
    y_expr = f"COUNT(`{yf}`)" if agg_count else f"`{yf}`"
    return {"widget": {"name": name,
                       "queries": [{"name": "main_query", "query": {
                           "datasetName": ds,
                           "fields": [{"name": xf, "expression": f"`{xf}`"}, {"name": y_name, "expression": y_expr}],
                           "disaggregated": not agg_count}}],
                       "spec": {"version": 3, "widgetType": kind,
                                "encodings": {"x": {"fieldName": xf, "scale": {"type": x_scale}, "displayName": xlab},
                                              "y": {"fieldName": y_name, "scale": {"type": "quantitative"},
                                                    "displayName": ylab}},
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def table(name, ds, cols, title, x, y, w, h):
    """cols: list of (field, display_name, type) with type in string|integer|float|datetime."""
    display_as = {"string": "string", "integer": "number", "float": "number", "datetime": "datetime"}
    columns = [{"fieldName": f, "displayName": d, "title": d, "type": t, "displayAs": display_as[t],
                "visible": True, "order": i, "alignContent": "left" if t == "string" else "right",
                "allowSearch": t == "string", "booleanValues": ["false", "true"],
                "imageUrlTemplate": "{{ @ }}", "imageTitleTemplate": "{{ @ }}", "imageWidth": "", "imageHeight": "",
                "linkUrlTemplate": "{{ @ }}", "linkTextTemplate": "{{ @ }}", "linkTitleTemplate": "{{ @ }}",
                "linkOpenInNewTab": True, "allowHTML": False, "highlightLinks": False,
                "useMonospaceFont": False, "preserveWhitespace": False}
               for i, (f, d, t) in enumerate(cols)]
    return {"widget": {"name": name,
                       "queries": [{"name": "main_query", "query": {
                           "datasetName": ds, "fields": [{"name": f, "expression": f"`{f}`"} for f, _, _ in cols],
                           "disaggregated": True}}],
                       "spec": {"version": 1, "widgetType": "table", "encodings": {"columns": columns},
                                "invisibleColumns": [], "allowHTMLByDefault": False, "itemsPerPage": 25,
                                "paginationSize": "default", "condensed": True, "withRowNumber": False,
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


def multi_filter(name, ds, field, title, x, y, w=2, h=2):
    qname = f"filter_{ds}_{field}"
    return {"widget": {"name": name,
                       "queries": [{"name": qname, "query": {
                           "datasetName": ds, "fields": [{"name": field, "expression": f"`{field}`"}],
                           "disaggregated": False}}],
                       "spec": {"version": 2, "widgetType": "filter-multi-select",
                                "encodings": {"fields": [{"fieldName": field, "displayName": title, "queryName": qname}]},
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}


# ---------- datasets: Gold only (+ ops health for freshness) ----------
datasets = [
    {"name": "ds_q1", "displayName": "Q1 · Population stats 2013–2018",
     "queryLines": [f"SELECT years_count, mean_population, stddev_population_sample, stddev_population_pop "
                    f"FROM {S}.gold_q1_population_stats"]},
    {"name": "ds_pop", "displayName": "US population by year",
     "queryLines": [f"SELECT year, population FROM {S}.silver_population ORDER BY year"]},
    {"name": "ds_q2", "displayName": "Q2 · Best year per series",
     "queryLines": [f"SELECT series_id, series_label, sector_name, measure_text, duration_text, best_year, "
                    f"CAST(best_year_value AS DOUBLE) AS best_year_total, periods_in_best_year, value_basis "
                    f"FROM {S}.gold_q2_series_best_year"]},
    {"name": "ds_q3", "displayName": "Q3 · Manufacturing hours (Q1) vs population",
     "queryLines": [f"SELECT year, CAST(value AS DOUBLE) AS pct_change_q1, population "
                    f"FROM {S}.gold_q3_prs30006032_q01_population ORDER BY year"]},
    {"name": "ds_health", "displayName": "Data freshness",
     "queryLines": [f"SELECT status, date_format(last_successful_refresh_at, 'yyyy-MM-dd HH:mm') AS last_refresh_utc, "
                    f"date_format(bls_source_last_modified, 'yyyy-MM-dd') AS bls_published "
                    f"FROM {S}.ops_pipeline_health"]},
]

# ---------- pages ----------
overview = [
    text("t_intro", [
        "# US Productivity & Population",
        "Answers built from the **U.S. Bureau of Labor Statistics** (Major Sector Productivity) and **Census population** "
        "data (via DataUSA). Use the tabs above: **Best year by series** and **Manufacturing hours vs population**.",
    ], 0, 0, 6, 2),
    counter("c_status", "ds_health", "status", "Data health", 0, 2),
    counter("c_refresh", "ds_health", "last_refresh_utc", "Last refreshed (UTC)", 2, 2),
    counter("c_bls", "ds_health", "bls_published", "BLS last published", 4, 2),
    counter("c_mean", "ds_q1", "mean_population", "Average US population, 2013–2018", 0, 5, 3, 3),
    counter("c_std", "ds_q1", "stddev_population_sample", "Standard deviation, 2013–2018", 3, 5, 3, 3),
    chart("ch_pop", "line", "ds_pop", "year", "population", "US population by year (2020 not published by Census)",
          "Year", "Population", 0, 8, 6, 6),
]

best_year = [
    text("t_q2", [
        "## In which year did each measure peak?",
        "For every BLS series, the **best year** is the year with the highest total across its quarters. "
        "For the few series published only as an annual average, that average is used (see *How it was calculated*). "
        "Filter by sector or measure, or search the table.",
    ], 0, 0, 6, 2),
    multi_filter("f_sector", "ds_q2", "sector_name", "Sector", 0, 2, 3, 2),
    multi_filter("f_measure", "ds_q2", "measure_text", "Measure", 3, 2, 3, 2),
    table("tb_q2", "ds_q2", [
        ("series_label", "What is measured", "string"),
        ("best_year", "Best year", "integer"),
        ("best_year_total", "Total in best year", "float"),
        ("value_basis", "How it was calculated", "string"),
        ("series_id", "BLS series ID", "string"),
    ], "Best year per series", 0, 4, 6, 9),
    chart("ch_bestyear", "bar", "ds_q2", "best_year", "series_id", "How many series peaked in each year",
          "Best year", "Number of series", 0, 13, 6, 6, agg_count=True),
]

hours_vs_pop = [
    text("t_q3", [
        "## Manufacturing hours worked vs US population",
        "**Series PRS30006032:** hours worked in manufacturing, % change from the previous quarter, "
        "shown for the **1st quarter** of each year. Population is shown where Census published it "
        "(from 2013; **2020 was not published** because of COVID data-quality issues).",
    ], 0, 0, 6, 2),
    chart("ch_q3", "line", "ds_q3", "year", "pct_change_q1",
          "Q1 change in manufacturing hours worked (%)", "Year", "% change vs previous quarter", 0, 2, 6, 6),
    table("tb_q3", "ds_q3", [
        ("year", "Year", "integer"),
        ("pct_change_q1", "Q1 % change in hours worked", "float"),
        ("population", "US population", "integer"),
    ], "Year by year", 0, 8, 6, 8),
]

about = [
    text("t_about", [
        "## About this data",
        "- **Sources:** BLS Major Sector Productivity and Costs (`download.bls.gov/pub/time.series/pr/`); "
        "US population from the Census ACS 1-year estimates via the DataUSA API.",
        "- **Refresh:** a daily job checks both sources and only reprocesses when something actually changed.",
        "- **Quality checks:** every load validates file structure and key fields; results are cross-checked "
        "in two independent ways.",
        "- **Reading the numbers:** many BLS series are *% changes* or *indexes*, so a series' 'total' is only "
        "meaningful within that series, never across series.",
        "- **Owner / questions:** Data Quest pipeline (`workspace.data_quest`).",
    ], 0, 0, 6, 6),
]

dashboard = {
    "datasets": datasets,
    "pages": [
        {"name": "overview", "displayName": "Overview", "layout": overview},
        {"name": "best_year", "displayName": "Best year by series", "layout": best_year},
        {"name": "hours_vs_population", "displayName": "Manufacturing hours vs population", "layout": hours_vs_pop},
        {"name": "about", "displayName": "About the data", "layout": about},
    ],
}

out = Path(__file__).with_name("data_quest_dashboard.lvdash.json")
out.write_text(json.dumps(dashboard, indent=2), encoding="utf-8")
print(f"wrote {out}")
