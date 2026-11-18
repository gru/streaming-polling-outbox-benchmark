#!/usr/bin/env python3
"""Генератор компактной сводной Markdown-таблицы сравнения Outbox CDC.

Читает отчёты results/benchmark/benchmark-<arm>.json (polling, competing,
streaming, walv6). На первый план выводит лаконичные наглядные таблицы,
а детальные срезы перцентилей и статистики СУБД убирает под <details>.
"""
import argparse
import json
import os
import sys

ARM_ORDER = [
    ("polling",   "1. polling",   "SQL peek слота (таблица)"),
    ("competing", "2. competing", "SKIP LOCKED, 10 воркеров (таблица)"),
    ("streaming", "3. streaming", "pgoutput text (таблица)"),
    ("walv6",     "4. walv6",     "pgoutput messages=true (без таблицы)"),
]


def pick(obj, *path, default=None):
    cur = obj
    for k in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    return cur if cur is not None else default


def fmt_int(v):
    if v is None:
        return "—"
    return f"{int(round(v)):,}".replace(",", " ")


def fmt_num(v, nd=1):
    if v is None:
        return "—"
    return f"{float(v):,.{nd}f}".replace(",", " ")


def write_mode_label(spec):
    wm = (spec or {}).get("writeMode") or "insertDelete"
    return {
        "insertDelete": "INSERT + DELETE",
        "insertOnly": "INSERT only",
        "walEmit": "WAL emit (без табл.)",
    }.get(wm, wm)


def pure_drain_rate(scen):
    written = pick(scen, "writer", "written")
    drain_ms = pick(scen, "timings", "drainMs")
    if written and drain_ms and drain_ms > 0:
        return written / (drain_ms / 1000.0)
    return pick(scen, "cdc", "deliveryRateAvgPerSecond")


def cdc_rate(scen):
    if not scen:
        return None
    return pick(scen, "cdc", "deliveryRateAvgPerSecond") or pure_drain_rate(scen)


def find_scen(report, prefix):
    for s in report.get("scenarios") or []:
        if (s.get("name") or "").startswith(prefix):
            return s
    return None


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    ap = argparse.ArgumentParser()
    ap.add_argument("--bench-dir", default="results/benchmark")
    ap.add_argument("--out", default="")
    ap.add_argument("--expected-arms", default="")
    ap.add_argument("--require-pass", action="store_true")
    args = ap.parse_args()

    reports = {}
    for arm, _, _ in ARM_ORDER:
        p = os.path.join(args.bench_dir, f"benchmark-{arm}.json")
        if os.path.isfile(p):
            with open(p, encoding="utf-8") as fh:
                reports[arm] = json.load(fh)

    if not reports:
        print("ОШИБКА: не найдено ни одного отчёта benchmark-<arm>.json", file=sys.stderr)
        return 1

    first = next(iter(reports.values()))
    vers = first.get("versions") or {}
    git_sha = (first.get("git") or {}).get("commit", "unknown")[:8]
    present_arms = [(a, lbl, arch) for a, lbl, arch in ARM_ORDER if a in reports]

    lines = [
        "# Сравнение реализаций Outbox → Kafka",
        "",
        f"**Стенд:** PostgreSQL `{vers.get('postgres', '17.2')}` + Patroni `{vers.get('patroni', '4.1.5')}` "
        "(3 узла, `sync_replication_slots=on`, прод-профиль `wal_compression=lz4`, `shared_buffers=512MB`, "
        "`logical_decoding_work_mem=256MB`) · Kafka `{}` (`acks=all`, `idempotence=true`, `CompressionType=None`) "
        "· коммит `{}`.".format(vers.get("kafka", "3.9.0"), git_sha),
        "",
    ]

    # --- 1. Компактная главная таблица s5 ---
    s5_any = next((find_scen(r, "s5") for r in reports.values() if find_scen(r, "s5")), None)
    if s5_any:
        s5_total = fmt_int(pick(s5_any, "spec", "totalMessages"))
        s5_size = fmt_int(pick(s5_any, "spec", "messageSizeBytes"))
        strm_s5 = find_scen(reports.get("streaming", {}), "s5") if "streaming" in reports else None
        comp_s5 = find_scen(reports.get("competing", {}), "s5") if "competing" in reports else None
        base_rate = cdc_rate(strm_s5) or cdc_rate(comp_s5)

        lines += [
            f"## 1. Пропускная способность (`s5`: {s5_total} накопленных сообщений × {s5_size} Б)",
            "",
            "| Вариант | Скорость CDC (avg / пик), msg/s | Ускорение | Время доставки, с | CPU лидера PG | WAL PG | Потери |",
            "|---|---:|---:|---:|---:|---:|---|",
        ]

        extra_s5 = []
        for arm, label, arch in present_arms:
            s = find_scen(reports[arm], "s5")
            if not s:
                continue
            rate = cdc_rate(s)
            peak = pick(s, "cdc", "deliveryRatePeakPerSecond")
            drain_ms = pick(s, "timings", "drainMs")
            drain_s = (drain_ms / 1000.0) if drain_ms else None
            sp = f"**{rate / base_rate:.2f}×**" if (rate and base_rate) else "—"
            pg_cpu = pick(s, "resources", "pgLeader", "cpuAvgPercent")
            pg_wal_mb = (pick(s, "postgres", "wal", "d_wal_bytes") or 0) / 1e6
            verd = pick(s, "verdict", "verdict", default="?")
            miss = pick(s, "verdict", "missing", default="?")
            ok_str = "✅ 0" if (verd == "pass" and miss == 0) else f"❌ {verd} (miss={miss})"
            is_wal = arm == "walv6"
            b = "**" if is_wal else ""

            lines.append(
                f"| {b}{label}{b} ({arch}) | {b}{fmt_int(rate)} / {fmt_int(peak)}{b} | {sp} | "
                f"{b}{fmt_num(drain_s, 1)} с{b} | {b}{fmt_num(pg_cpu, 1)}%{b} | "
                f"{b}{fmt_num(pg_wal_mb, 0)} МБ{b} | {ok_str} |"
            )

            cdc_ram = (pick(s, "resources", "cdc", "memMaxBytes") or 0) / (1024 * 1024)
            cdc_cpu = pick(s, "resources", "cdc", "cpuAvgPercent")
            w_rate = pick(s, "writer", "writerRatePerSecond")
            tup_del = pick(s, "postgres", "db", "d_tup_deleted")
            dup = pick(s, "verdict", "duplicates")
            extra_s5.append(
                f"| **{label}** | `{write_mode_label(s.get('spec'))}` | {fmt_int(w_rate)} | "
                f"{fmt_num(cdc_cpu, 1)}% | {fmt_num(cdc_ram, 0) if cdc_ram else '—'} | "
                f"{fmt_int(tup_del)} | {fmt_int(dup)} |"
            )

        lines += [
            "",
            "<details>",
            "<summary>Дополнительные метрики s5 (RAM и CPU CDC, запись Writer, удалённые строки)</summary>",
            "",
            "| Вариант | Путь записи | Writer запись, msg/s | CDC CPU avg | CDC RAM пик, МБ | Удалено строк | Дубликатов |",
            "|---|---|---:|---:|---:|---:|---:|",
            *extra_s5,
            "",
            "</details>",
            "",
        ]

    # --- 2. Сквозная задержка s1..s4: компактная сводная матрица 4 строки + <details> ---
    other_scens = [
        ("s1", "s1 — равномерный 50 msg/s (256 Б)"),
        ("s2", "s2 — равномерный 200 msg/s (256 Б)"),
        ("s3", "s3 — залп без лимита (256 Б)"),
        ("s4", "s4 — крупные 4 КБ @ 50 msg/s"),
    ]
    if any(find_scen(r, p) for p, _ in other_scens for r in reports.values()):
        hdr = "| Сценарий | " + " | ".join(f"`{lbl}`" for _, lbl, _ in present_arms) + " |"
        sep = "|---|" + "|".join("---:" for _ in present_arms) + "|"
        lines += [
            "## 2. Средняя сквозная задержка `e2e mean` по сценариям `s1`–`s4`",
            "",
            hdr,
            sep,
        ]
        detail_rows = []
        for pref, desc in other_scens:
            cells = [f"**{desc}**"]
            for arm, label, _ in present_arms:
                s = find_scen(reports[arm], pref)
                if not s:
                    cells.append("—")
                    continue
                mean_ms = pick(s, "cdc", "e2eMeanMs")
                cells.append(f"**{fmt_num(mean_ms, 1)} мс**" if arm == "walv6" else f"{fmt_num(mean_ms, 1)} мс")

                written = pick(s, "writer", "written")
                rate_avg = pick(s, "cdc", "deliveryRateAvgPerSecond") or pick(s, "cdc", "deliveryRateOverallPerSecond")
                p50 = pick(s, "cdc", "e2eP50Ms")
                p95 = pick(s, "cdc", "e2eP95Ms")
                p99 = pick(s, "cdc", "e2eP99Ms")
                drain_ms = pick(s, "timings", "drainMs")
                pg_cpu = pick(s, "resources", "pgLeader", "cpuAvgPercent")
                pg_wal_mb = (pick(s, "postgres", "wal", "d_wal_bytes") or 0) / 1e6
                verd = pick(s, "verdict", "verdict", default="?")
                detail_rows.append(
                    f"| `{pref}` | **{label}** | {fmt_int(written)} | {fmt_num(rate_avg, 1)} | "
                    f"{fmt_num(mean_ms, 1)} | {fmt_num(p50, 1)} / {fmt_num(p95, 1)} / {fmt_num(p99, 1)} | "
                    f"{fmt_int(drain_ms)} | {fmt_num(pg_cpu, 1)}% | {fmt_num(pg_wal_mb, 1)} | {verd} |"
                )
            lines.append("| " + " | ".join(cells) + " |")

        lines += [
            "",
            "<details>",
            "<summary>Развернуть полную таблицу перцентилей s1–s4 (p50 / p95 / p99, доставка остатка, CPU, WAL)</summary>",
            "",
            "| Сценарий | Вариант | Сообщений | Скорость avg, msg/s | e2e mean, мс | e2e p50 / p95 / p99, мс | Доставка остатка, мс | PG Leader CPU | PG WAL, МБ | Вердикт |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---|",
            *detail_rows,
            "",
            "</details>",
            "",
        ]

    # --- 3. Внутренняя телеметрия PostgreSQL и сводка цикла под <details> ---
    lines += [
        "## 3. Сводка целостности и внутренняя статистика PostgreSQL",
        "",
        "| Вариант | Сценариев pass | Доставлено всего | Ошибки (чтение / Kafka) | Записей в WAL (`d_wal_records`) | Чтений буферов (`d_blks_hit`) | Суммарно WAL, МБ |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    pg_detail = []
    for arm, label, _ in present_arms:
        r = reports[arm]
        t = r.get("totals") or {}
        sc_cnt = t.get("scenarios", 0)
        sc_pass = t.get("verdictsPass", 0)
        pass_disp = f"✅ {sc_pass}/{sc_cnt}" if sc_cnt > 0 and sc_pass == sc_cnt else f"❌ {sc_pass}/{sc_cnt}"
        wal_rec = pick(r, "postgres", "wal", "d_wal_records")
        blks_hit = pick(r, "postgres", "db", "d_blks_hit")
        wal_tot = (pick(r, "postgres", "wal", "d_wal_bytes") or 0) / 1e6
        lines.append(
            f"| **{label}** | {pass_disp} | {fmt_int(t.get('messagesDelivered'))} | "
            f"{fmt_int(t.get('readErrors'))} / {fmt_int(t.get('kafkaErrors'))} | "
            f"{fmt_int(wal_rec)} | {fmt_int(blks_hit)} | {fmt_num(wal_tot, 1)} |"
        )

        stmts = pick(r, "postgres", "statements") or []
        exec_ms = sum(s.get("d_total_exec_ms") or 0 for s in stmts)
        dirtied = sum(s.get("d_shared_blks_dirtied") or 0 for s in stmts)
        temp_mb = (pick(r, "postgres", "db", "d_temp_bytes") or 0) / 1e6
        tup_ret = pick(r, "postgres", "db", "d_tup_returned")
        tup_del = pick(r, "postgres", "db", "d_tup_deleted")
        pg_detail.append(
            f"| **{label}** | {fmt_num(exec_ms, 1)} | {fmt_int(dirtied)} | "
            f"{fmt_int(tup_ret)} | {fmt_int(tup_del)} | {fmt_num(temp_mb, 1)} |"
        )

    lines += [
        "",
        "<details>",
        "<summary>Развернуть детальную нагрузку на ядро PostgreSQL (SQL exec ms, dirty buffers, temp files)</summary>",
        "",
        "| Вариант | Суммарное время SQL в БД, мс | Грязных страниц (`dirtied`) | Просканировано строк (`tup_returned`) | Удалено строк (`tup_deleted`) | Temp-файлы на диске, МБ |",
        "|---|---:|---:|---:|---:|---:|",
        *pg_detail,
        "",
        "</details>",
        "",
    ]

    md_text = "\n".join(lines) + "\n"
    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(md_text)
    sys.stdout.write(md_text)

    if args.expected_arms:
        exp = [x.strip() for x in args.expected_arms.split(",") if x.strip()]
        missing_arms = [a for a in exp if a not in reports]
        if missing_arms:
            print(f"ОШИБКА: отсутствуют отчёты для вариантов: {', '.join(missing_arms)}", file=sys.stderr)
            return 2

    if args.require_pass:
        for arm, r in reports.items():
            t = r.get("totals") or {}
            if not t.get("scenarios") or t.get("verdictsPass") != t.get("scenarios"):
                print(
                    f"ОШИБКА: не все сценарии прошли с вердиктом pass у варианта {arm}: "
                    f"{t.get('verdictsPass')}/{t.get('scenarios')}",
                    file=sys.stderr,
                )
                return 3

    return 0


if __name__ == "__main__":
    sys.exit(main())
