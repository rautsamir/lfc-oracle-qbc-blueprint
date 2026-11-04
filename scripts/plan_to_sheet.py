"""Publish the inventory and pipeline plan to a Google Sheet for tracking.

Usage:
    GOOGLE_TOKEN=$(gcloud auth print-access-token) \
      python scripts/plan_to_sheet.py inventory/tables.csv inventory/plan.csv "ERP QBC Migration Tracker"

Optional: GOOGLE_QUOTA_PROJECT=<project> if your token needs a quota project header.

Tabs:
  Plan       one row per pipeline: tier, tables, GB, estimate, status, failure class (A/B/C)
  Inventory  one row per source table, sorted by size, with cumulative % of bytes
  Summary    count vs. volume progress, so "99% of tables" is never read as "99% of data"
"""
import csv
import json
import os
import sys
import urllib.request

TOKEN = os.environ.get("GOOGLE_TOKEN") or sys.exit("Set GOOGLE_TOKEN")
HDR = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
if os.environ.get("GOOGLE_QUOTA_PROJECT"):
    HDR["x-goog-user-project"] = os.environ["GOOGLE_QUOTA_PROJECT"]
API = "https://sheets.googleapis.com/v4/spreadsheets"


def call(method, url, body=None):
    req = urllib.request.Request(url, method=method, headers=HDR, data=json.dumps(body).encode() if body else None)
    return json.load(urllib.request.urlopen(req))


def rgb(h):
    return {"red": int(h[0:2], 16) / 255, "green": int(h[2:4], 16) / 255, "blue": int(h[4:6], 16) / 255}


def main(inv_path, plan_path, title):
    inv = sorted(csv.DictReader(open(inv_path)), key=lambda r: -float(r.get("size_gb") or 0))
    plan = list(csv.DictReader(open(plan_path)))
    total = sum(float(r.get("size_gb") or 0) for r in inv) or 1

    inv_head = ["source_table", "size_gb", "cum_pct_bytes", "tier", "primary_keys", "cursor_column", "cursor_indexed", "partition_column", "deletes", "notes", "pipeline"]
    table_to_pipe = {t: p["pipeline"] for p in plan for t in p["table_list"].split()}
    inv_rows, cum = [], 0.0
    for r in inv:
        cum += float(r.get("size_gb") or 0)
        inv_rows.append([r["source_table"].upper(), float(r.get("size_gb") or 0), round(100 * cum / total, 1), r.get("tier", ""),
                         r.get("primary_keys", ""), r.get("cursor_column", ""), r.get("cursor_indexed", ""), r.get("partition_column", ""),
                         r.get("deletes", ""), r.get("notes", ""), table_to_pipe.get(r["source_table"].upper(), "")])

    plan_head = list(plan[0].keys())
    plan_rows = [[p[k] for k in plan_head] for p in plan]
    for row in plan_rows:
        for i, k in enumerate(plan_head):
            if k in ("tables", "total_gb", "largest_gb", "est_hours"):
                row[i] = float(row[i])

    n = len(plan_rows) + 1
    st = chr(ord("A") + plan_head.index("status"))
    gb = chr(ord("A") + plan_head.index("total_gb"))
    tb = chr(ord("A") + plan_head.index("tables"))
    summary = [
        ["Metric", "Value"],
        ["Pipelines", f"=COUNTA(Plan!A2:A{n})"],
        ["Pipelines completed", f'=COUNTIF(Plan!{st}2:{st}{n},"Completed")'],
        ["Tables total", f"=SUM(Plan!{tb}2:{tb}{n})"],
        ["Tables in completed pipelines", f'=SUMIF(Plan!{st}2:{st}{n},"Completed",Plan!{tb}2:{tb}{n})'],
        ["% of tables", "=IFERROR(B5/B4,0)"],
        ["GB total", f"=SUM(Plan!{gb}2:{gb}{n})"],
        ["GB in completed pipelines", f'=SUMIF(Plan!{st}2:{st}{n},"Completed",Plan!{gb}2:{gb}{n})'],
        ["% of volume", "=IFERROR(B8/B7,0)"],
        ["Failed pipelines, class A (config)", f'=COUNTIFS(Plan!{st}2:{st}{n},"Failed",Plan!{chr(ord("A") + plan_head.index("failure_class"))}2:{chr(ord("A") + plan_head.index("failure_class"))}{n},"A")'],
        ["Failed pipelines, class B (connection)", f'=COUNTIFS(Plan!{st}2:{st}{n},"Failed",Plan!{chr(ord("A") + plan_head.index("failure_class"))}2:{chr(ord("A") + plan_head.index("failure_class"))}{n},"B")'],
        ["Failed pipelines, class C (shuffle)", f'=COUNTIFS(Plan!{st}2:{st}{n},"Failed",Plan!{chr(ord("A") + plan_head.index("failure_class"))}2:{chr(ord("A") + plan_head.index("failure_class"))}{n},"C")'],
    ]

    ss = call("POST", API, {"properties": {"title": title}, "sheets": [
        {"properties": {"title": t, "gridProperties": {"frozenRowCount": 1}}} for t in ("Plan", "Inventory", "Summary")]})
    sid = ss["spreadsheetId"]
    gids = {s["properties"]["title"]: s["properties"]["sheetId"] for s in ss["sheets"]}
    call("POST", f"{API}/{sid}/values:batchUpdate", {"valueInputOption": "USER_ENTERED", "data": [
        {"range": "Plan!A1", "values": [plan_head] + plan_rows},
        {"range": "Inventory!A1", "values": [inv_head] + inv_rows},
        {"range": "Summary!A1", "values": summary},
    ]})

    reqs = []
    for t, gid in gids.items():
        reqs.append({"repeatCell": {"range": {"sheetId": gid, "startRowIndex": 0, "endRowIndex": 1},
                                    "cell": {"userEnteredFormat": {"backgroundColor": rgb("1C3A5E"), "textFormat": {"bold": True, "foregroundColor": rgb("FFFFFF")}}},
                                    "fields": "userEnteredFormat"}})
        reqs.append({"autoResizeDimensions": {"dimensions": {"sheetId": gid, "dimension": "COLUMNS", "startIndex": 0, "endIndex": 12}}})
    pg, si = gids["Plan"], plan_head.index("status")
    reqs.append({"setDataValidation": {"range": {"sheetId": pg, "startRowIndex": 1, "endRowIndex": 2000, "startColumnIndex": si, "endColumnIndex": si + 1},
                                       "rule": {"condition": {"type": "ONE_OF_LIST", "values": [{"userEnteredValue": v} for v in ["Not started", "Running", "Completed", "Mostly completed", "Failed", "Error"]]}, "showCustomUi": True}}})
    fi = plan_head.index("failure_class")
    reqs.append({"setDataValidation": {"range": {"sheetId": pg, "startRowIndex": 1, "endRowIndex": 2000, "startColumnIndex": fi, "endColumnIndex": fi + 1},
                                       "rule": {"condition": {"type": "ONE_OF_LIST", "values": [{"userEnteredValue": v} for v in ["A", "B", "C"]]}, "showCustomUi": True}}})
    for text, color in [("Completed", "B7E1B5"), ("Failed", "E8B4B0"), ("Error", "E8B4B0"), ("Mostly completed", "F8E6A0"), ("Running", "CFE2F3")]:
        reqs.append({"addConditionalFormatRule": {"index": 0, "rule": {
            "ranges": [{"sheetId": pg, "startRowIndex": 1, "endRowIndex": 2000, "startColumnIndex": si, "endColumnIndex": si + 1}],
            "booleanRule": {"condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": text}]}, "format": {"backgroundColor": rgb(color)}}}}})
    for tier, color in [("S", "EAF6EC"), ("M", "E8F1FB"), ("L", "FFF6E0"), ("XL", "FDECEA")]:
        reqs.append({"addConditionalFormatRule": {"index": 0, "rule": {
            "ranges": [{"sheetId": pg, "startRowIndex": 1, "endRowIndex": 2000, "startColumnIndex": 1, "endColumnIndex": 2}],
            "booleanRule": {"condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": tier}]}, "format": {"backgroundColor": rgb(color)}}}}})
    reqs.append({"repeatCell": {"range": {"sheetId": gids["Summary"], "startRowIndex": 5, "endRowIndex": 6, "startColumnIndex": 1, "endColumnIndex": 2},
                                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}}, "fields": "userEnteredFormat.numberFormat"}})
    reqs.append({"repeatCell": {"range": {"sheetId": gids["Summary"], "startRowIndex": 8, "endRowIndex": 9, "startColumnIndex": 1, "endColumnIndex": 2},
                                "cell": {"userEnteredFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0.0%"}}}, "fields": "userEnteredFormat.numberFormat"}})
    reqs.append({"setBasicFilter": {"filter": {"range": {"sheetId": pg, "startRowIndex": 0, "endRowIndex": n, "startColumnIndex": 0, "endColumnIndex": len(plan_head)}}}})
    call("POST", f"{API}/{sid}:batchUpdate", {"requests": reqs})
    print(f"https://docs.google.com/spreadsheets/d/{sid}/edit")


if __name__ == "__main__":
    main(*sys.argv[1:4])
