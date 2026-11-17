#!/usr/bin/env python3
"""make-sweep-summary.py — сводка batch/workers-sweep по сохранённым отчётам.

Читает results/benchmark/benchmark-<arm>-b<batch>[-w<workers>][-failed].json
(снапшоты каждого цикла серии) и собирает:
  results/benchmark/sweep-summary.json  — машиночитаемо: конфигурация + totals +
                                          все ключевые метрики по 5 сценариям;
  results/benchmark/sweep-summary.md    — человекочитаемые таблицы для выводов.
"""
import glob
import json
import os
import re
import sys

BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
BENCH_DIR = os.path.join(BASE, "results", "benchmark")

NAME_RE = re.compile(
    r"benchmark-(?P<arm>polling|streaming|competing|walv6)"
    r"-b(?P<batch>\d+)(?:-w(?P<workers>\d+))?(?P<failed>-failed)?\.json$"
)


def pick(obj, *path, default=None):
    for k in path:
        if not isinstance(obj, dict):
            return default
        obj = obj.get(k)
    return obj if obj is not None else default


def fmt(v, nd=1):
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:,.{nd}f}".replace(",", " ")
    return f"{v:,}".replace(",", " ") if isinstance(v, int) else str(v)


def load_config_files():
    out = []
    for path in sorted(glob.glob(os.path.join(BENCH_DIR, "benchmark-*-b*.json"))):
        m = NAME_RE.search(os.path.basename(path))
        if not m:
            continue
        try:
            data = json.load(open(path))
        except Exception as e:
            out.append({"file": os.path.basename(path), "error": str(e),
                        "arm": m.group("arm"), "batch": int(m.group("batch")),
                        "workers": int(m.group("workers")) if m.group("workers") else None,
                        "failed": bool(m.group("failed"))})
            continue
        out.append({
            "file": os.path.basename(path),
            "arm": m.group("arm"),
            "batch": int(m.group("batch")),
            "workers": int(m.group("workers")) if m.group("workers") else None,
            "failed": bool(m.group("failed")),
            "data": data,
        })
    # сортировка: arm → batch → workers
    order = {"polling": 0, "streaming": 1, "competing": 2, "walv6": 3}
    out.sort(key=lambda c: (order.get(c["arm"], 9), c["batch"], c["workers"] or 0))
    return out


def scenario_rows(cfg):
    rows = []
    for s in pick(cfg.get("data"), "scenarios", default=[]):
        c = s.get("cdc") or {}
        t = s.get("timings") or {}
        v = s.get("verdict") or {}
        w = s.get("writer") or {}
        pg = s.get("postgres") or {}
        lead = pick(s, "resources", "pgLeader") or {}
        rows.append({
            "scenario": s.get("name"),
            "written": w.get("written"),
            "writerRatePerSecond": w.get("writerRatePerSecond"),
            "deliveryTotalMs": t.get("deliveryTotalMs"),
            "drainMs": t.get("drainMs"),
            "cdcBootMs": t.get("cdcBootMs"),
            "rateOverall": c.get("deliveryRateOverallPerSecond"),
            "rateAvg": c.get("deliveryRateAvgPerSecond"),
            "ratePeak": c.get("deliveryRatePeakPerSecond"),
            "e2eMeanMs": c.get("e2eMeanMs"),
            "e2eP50Ms": c.get("e2eP50Ms"),
            "e2eP95Ms": c.get("e2eP95Ms"),
            "e2eP99Ms": c.get("e2eP99Ms"),
            "verdict": v.get("verdict"),
            "duplicates": v.get("duplicates"),
            "missing": v.get("missing"),
            "readErrors": c.get("readErrors"),
            "kafkaErrors": c.get("kafkaErrors"),
            "pgLeaderCpuAvgPercent": lead.get("cpuAvgPercent"),
            "pgLeaderCpuMaxPercent": lead.get("cpuMaxPercent"),
            "pgWalMb": round((pick(pg, "wal", "d_wal_bytes") or 0) / 1e6, 1),
        })
    return rows


def main():
    cfgs = load_config_files()
    if not cfgs:
        print("нет файлов benchmark-*-b*.json — сводку не из чего собирать")
        return 1

    summary = {"schema": "streaming-polling-outbox/benchmark-sweep/v1", "runs": []}
    md = ["# Batch/workers-sweep: сводка прогонов", "",
          "Каждая строка конфигурации — полный цикл (5 сценариев s1–s5) на пересозданном",
          "стенде. Файл отчёта: `results/benchmark/benchmark-<arm>-<tag>.json`, полные",
          "артефакты: `results/raw/<stamp>-benchmark-<arm>/`.", ""]

    for cfg in cfgs:
        tag = f"b{cfg['batch']}" + (f"-w{cfg['workers']}" if cfg["workers"] else "")
        entry = {"file": cfg["file"], "arm": cfg["arm"], "batch": cfg["batch"],
                 "workers": cfg["workers"], "failedSnapshot": cfg["failed"]}
        if "data" in cfg:
            entry["startedAt"] = cfg["data"].get("startedAt")
            entry["totals"] = pick(cfg["data"], "totals", default={})
            entry["scenarios"] = scenario_rows(cfg)
        else:
            entry["error"] = cfg.get("error")
        summary["runs"].append(entry)

        md += [f"## {cfg['arm']} — batch {cfg['batch']}"
               + (f", воркеры {cfg['workers']}" if cfg["workers"] else "")
               + f" `{tag}`"
               + (" — НЕУСПЕШНЫЙ ЦИКЛ (частичный снапшот)" if cfg["failed"] else ""), ""]
        if "data" not in cfg:
            md += [f"ошибка чтения: {cfg.get('error')}", ""]
            continue
        t = entry.get("totals") or {}
        md += [
            f"старт: {entry.get('startedAt','?')} | сценариев {t.get('scenarios','?')}, "
            f"pass {t.get('verdictsPass','?')}/{t.get('scenarios','?')}, "
            f"доставлено {fmt(t.get('messagesDelivered'),0)}, "
            f"дубликатов {t.get('duplicatesTotal')}, "
            f"readErrors {t.get('readErrors')}, kafkaErrors {t.get('kafkaErrors')}", "",
            "| сценарий | overall, msg/s | avg, msg/s | пик, msg/s | e2e mean, мс | e2e p95, мс | дрейн, мс | вердикт | дубл. |",
            "|---|---:|---:|---:|---:|---:|---:|---|---:|",
        ]
        for r in entry["scenarios"]:
            short = (r["scenario"] or "?").split("-")[0]
            md.append(
                f"| {short} | {fmt(r['rateOverall'],1)} | {fmt(r['rateAvg'],1)} | "
                f"{fmt(r['ratePeak'],1)} | {fmt(r['e2eMeanMs'],1)} | {fmt(r['e2eP95Ms'],1)} | "
                f"{fmt(r['drainMs'],0)} | {r['verdict']} | {r['duplicates']} |")
        md.append("")

    # сравнительная таблица s5 (главная для выводов по throughput)
    md += ["## s5 (дрейн 4M pre-seeded) — все конфигурации рядом", "",
           "| arm | batch | воркеры | дрейн, с | avg, msg/s | пик, msg/s | e2e mean, мс | CPU лидера avg, % | WAL, ГБ | вердикт | файл |",
           "|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|"]
    for entry in summary["runs"]:
        if "scenarios" not in entry:
            continue
        s5 = next((r for r in entry["scenarios"] if (r["scenario"] or "").startswith("s5")), None)
        if not s5:
            continue
        md.append(
            f"| {entry['arm']} | {entry['batch']} | {entry['workers'] or '—'} | "
            f"{fmt((s5['drainMs'] or 0)/1000, 1)} | {fmt(s5['rateAvg'],0)} | "
            f"{fmt(s5['ratePeak'],0)} | {fmt(s5['e2eMeanMs'],0)} | "
            f"{fmt(s5['pgLeaderCpuAvgPercent'],1)} | {fmt(s5['pgWalMb']/1000, 2)} | "
            f"{s5['verdict']} | `{entry['file']}` |")
    md.append("")

    json.dump(summary, open(os.path.join(BENCH_DIR, "sweep-summary.json"), "w"),
              ensure_ascii=False, indent=2, sort_keys=True)
    open(os.path.join(BENCH_DIR, "sweep-summary.md"), "w").write("\n".join(md))
    print(f"сводка: {os.path.join(BENCH_DIR, 'sweep-summary.json')}")
    print(f"сводка: {os.path.join(BENCH_DIR, 'sweep-summary.md')}")
    print(f"конфигураций: {len(summary['runs'])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
