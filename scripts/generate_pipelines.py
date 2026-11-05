"""Turn an Oracle table inventory into tiered Lakeflow Connect QBC pipelines and a run plan.

Usage:
    python scripts/generate_pipelines.py inventory/tables.csv resources/ [--plan inventory/plan.csv]

Steps:
  1. Load and check the inventory (uppercase identifiers, cursor present, cursor indexed on big tables).
  2. Assign a tier from size_gb when the tier column is blank (override by filling it in).
  3. Pack each tier largest-first into pipelines, capped by table count AND total GB, so
     pipelines in a tier finish at similar times.
  4. Give XL tables, tables with a partition override, and isolate=Y tables their own pipeline.
  5. Write one YAML per pipeline, a job (S first as a smoke test, then M/L/XL in parallel so the
     biggest table, the critical path, starts as early as possible), and a plan CSV.

Thresholds come from a ~1,700-table, 4 TB ERP migration. Tune TIERS for your estate after the
pilot, and recalibrate GB_PER_HOUR from real runs.
"""
import argparse
import csv
import sys
from pathlib import Path

# tier: (max size_gb for auto-tiering, max tables per pipeline, max total GB per pipeline)
TIERS = {
    "S": (1, 50, 25),
    "M": (50, 15, 250),
    "L": (200, 4, 500),
    "XL": (float("inf"), 1, float("inf")),
}
TIER_ORDER = ["S", "M", "L", "XL"]
MAX_TABLES_PER_PIPELINE = 100
HARD_DELETE_INTERVAL_S = 86400  # lower bound between PK snapshot scans; tune to business tolerance
GB_PER_HOUR = 40.0             # rough single-table throughput; recalibrate after the pilot
PARALLEL_TABLES = 4            # tables a pipeline effectively reads at once
OVERHEAD_H_PER_TABLE = 0.02    # setup, metadata queries, merge


def auto_tier(size_gb):
    for t in TIER_ORDER:
        if size_gb <= TIERS[t][0]:
            return t
    return "XL"


def load(path):
    rows = list(csv.DictReader(open(path)))
    errors = []
    for r in rows:
        r["source_table"] = r["source_table"].strip().upper()
        r["primary_keys"] = [k.strip().upper() for k in (r.get("primary_keys") or "").split("|") if k.strip()]
        r["cursor_column"] = (r.get("cursor_column") or "").strip().upper()
        r["partition_column"] = (r.get("partition_column") or "").strip().upper()
        r["size_gb"] = float(r.get("size_gb") or 0)
        r["tier"] = (r.get("tier") or "").strip().upper() or auto_tier(r["size_gb"])
        r["isolate"] = (r.get("isolate") or "").strip().upper() == "Y"
        r["deletes"] = (r.get("deletes") or "none").strip().lower()
        name = r["source_table"]
        if r["tier"] not in TIERS:
            errors.append(f"{name}: unknown tier {r['tier']}")
        if not r["cursor_column"]:
            errors.append(f"{name}: no cursor column (every run would be a full load)")
        if r["tier"] in ("L", "XL") and (r.get("cursor_indexed") or "").strip().upper() != "Y":
            errors.append(f"{name}: cursor {r['cursor_column']} not indexed on a {r['tier']} table")
        if r["deletes"] == "hard" and not r["primary_keys"]:
            errors.append(f"{name}: hard deletes need primary keys")
        if r["partition_column"] and r["partition_column"] == r["cursor_column"]:
            errors.append(f"{name}: partition column should not be the cursor column")
    if errors:
        sys.exit("Inventory errors:\n  " + "\n  ".join(errors))
    return rows


def pack(tier, rows):
    """First-fit decreasing by GB, capped by table count and total GB per pipeline."""
    _, max_n, max_gb = TIERS[tier]
    max_n = min(max_n, MAX_TABLES_PER_PIPELINE)
    bins = []
    for r in sorted(rows, key=lambda x: -x["size_gb"]):
        for b in bins:
            if len(b) < max_n and sum(t["size_gb"] for t in b) + r["size_gb"] <= max_gb:
                b.append(r)
                break
        else:
            bins.append([r])
    return bins


def est_hours(tables):
    gb = [t["size_gb"] for t in tables]
    read = max(max(gb) / GB_PER_HOUR, sum(gb) / (GB_PER_HOUR * min(len(gb), PARALLEL_TABLES)))
    return round(read + OVERHEAD_H_PER_TABLE * len(gb), 1)


def table_block(r):
    lines = [
        "          - table:",
        "              source_catalog: ${var.source_catalog}",
        "              source_schema: ${var.source_schema}",
        f"              source_table: {r['source_table']}",
        "              destination_catalog: ${var.dest_catalog}",
        "              destination_schema: ${var.dest_schema}",
        f"              destination_table: {r['source_table'].lower()}",
        "              table_configuration:",
        "                scd_type: SCD_TYPE_1",
    ]
    if r["primary_keys"]:
        lines.append("                primary_keys:")
        lines += [f"                  - {k}" for k in r["primary_keys"]]
    lines += [
        "                query_based_connector_config:",
        "                  cursor_columns:",
        f"                    - {r['cursor_column']}",
    ]
    if r["deletes"] == "hard":
        lines.append(f"                  hard_deletion_sync_min_interval_in_seconds: {HARD_DELETE_INTERVAL_S}")
    return "\n".join(lines)


def pipeline_yaml(key, tables, partition_column=None, classic=False):
    out = [
        "resources:",
        "  pipelines:",
        f"    {key}:",
        f"      name: ${{bundle.target}}_{key}",
        "      catalog: ${var.dest_catalog}",
        "      target: ${var.dest_schema}",
    ]
    if classic:
        # Classic compute for QBC is Beta. Use it when only classic networking can reach Oracle.
        out += ["      serverless: false", "      clusters:", "        - label: default",
                "          autoscale:", "            min_workers: 1", "            max_workers: 4"]
    else:
        out += ["      serverless: true"]
    if partition_column:
        out += ["      configuration:", f"        dlt-pipelines.qbcPartitionColumn: {partition_column}"]
    out += ["      ingestion_definition:", "        connection_name: ${var.connection_name}", "        objects:"]
    out += [table_block(t) for t in tables]
    return "\n".join(out) + "\n"


def job_yaml(keys_by_tier):
    # S runs first as a smoke test of connectivity and config. M, L, and XL then start
    # together: the biggest XL table is the critical path, so it must not wait behind L.
    out = [
        "resources:",
        "  jobs:",
        "    qbc_orchestration:",
        "      name: ${bundle.target}_qbc_orchestration",
        "      max_concurrent_runs: 1",
        "      # Time-based trigger. Paused by default so the first run is deliberate.",
        "      schedule:",
        "        quartz_cron_expression: ${var.schedule_cron}",
        "        timezone_id: ${var.schedule_timezone}",
        "        pause_status: PAUSED",
        "      # Event-based alternative: start when an upstream table updates (for example a",
        "      # control table the source's nightly batch writes). Replace schedule with:",
        "      # trigger:",
        "      #   table_update:",
        "      #     table_names:",
        "      #       - ${var.dest_catalog}.ops.source_batch_complete",
        "      queue:",
        "        enabled: true",
        "      email_notifications:",
        "        on_failure:",
        "          - ${var.notify_email}",
        "      tags:",
        "        bundle: ${bundle.name}",
        "        target: ${bundle.target}",
        "      tasks:",
    ]
    small = keys_by_tier.get("S", [])
    for tier in TIER_ORDER:
        for key in keys_by_tier.get(tier, []):
            out += [f"        - task_key: {key}"]
            if tier != "S" and small:
                out += ["          depends_on:"] + [f"            - task_key: {s}" for s in small]
            out += [
                "          pipeline_task:",
                f"            pipeline_id: ${{resources.pipelines.{key}.id}}",
                "            full_refresh: false  # never full-refresh from the scheduler",
            ]
    return "\n".join(out) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("inventory")
    ap.add_argument("outdir")
    ap.add_argument("--plan", default=None, help="write a plan CSV (one row per pipeline)")
    ap.add_argument("--classic", action="store_true", help="classic pipeline compute instead of serverless (QBC Beta)")
    a = ap.parse_args()

    rows = load(a.inventory)
    outdir = Path(a.outdir)
    outdir.mkdir(exist_ok=True)
    for f in outdir.glob("qbc_*.yml"):
        f.unlink()

    keys_by_tier, plan = {}, []
    for tier in TIER_ORDER:
        tier_rows = [r for r in rows if r["tier"] == tier]
        solo = [r for r in tier_rows if r["partition_column"] or r["isolate"]]
        groups = pack(tier, [r for r in tier_rows if not (r["partition_column"] or r["isolate"])])
        groups += [[r] for r in solo]
        for n, g in enumerate(groups):
            key = f"qbc_{tier.lower()}_{n:02d}" + (f"_{g[0]['source_table'].lower()}" if len(g) == 1 and tier in ("L", "XL") else "")
            pc = g[0]["partition_column"] if len(g) == 1 else None
            (outdir / f"{key}.yml").write_text(pipeline_yaml(key, g, pc, a.classic))
            keys_by_tier.setdefault(tier, []).append(key)
            plan.append({
                "pipeline": key, "tier": tier, "tables": len(g),
                "total_gb": round(sum(t["size_gb"] for t in g), 1),
                "largest_gb": round(max(t["size_gb"] for t in g), 1),
                "est_hours": est_hours(g),
                "partition_override": pc or "",
                "table_list": " ".join(t["source_table"] for t in g),
            })
    (outdir / "qbc_orchestration_job.yml").write_text(job_yaml(keys_by_tier))

    total_gb = sum(r["size_gb"] for r in rows)
    s_hours = max((p["est_hours"] for p in plan if p["tier"] == "S"), default=0)
    rest = max((p["est_hours"] for p in plan if p["tier"] != "S"), default=0)
    print(f"{len(rows)} tables, {total_gb:,.0f} GB -> {len(plan)} pipelines: "
          + ", ".join(f"{t}={len(keys_by_tier.get(t, []))}" for t in TIER_ORDER))
    print(f"Rough full-load estimate: {s_hours + rest:.1f} h (S {s_hours:.1f} h, then critical path {rest:.1f} h)")

    if a.plan:
        with open(a.plan, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(plan[0].keys()) + ["status", "pipeline_id", "failure_class", "last_run_hours", "notes"])
            w.writeheader()
            for p in plan:
                w.writerow({**p, "status": "Not started", "pipeline_id": "", "failure_class": "", "last_run_hours": "", "notes": ""})
        print(f"Plan written to {a.plan}")


if __name__ == "__main__":
    main()
