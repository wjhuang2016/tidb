#!/usr/bin/env python3
"""High-throughput Go-vs-Rust differential runner (persistent connections).

Usage: fastdiff.py <casefile> [--no-warn]
Casefile: '#' comments; statements one per line (or semicolon-joined lines are
split). Runs each statement in sequence on a FRESH database per server,
capturing result/error/warnings per statement. Diffs the two transcripts.

Fresh-db discipline: the case may start with statements naming the db
qualified (db.tbl) or a USE; the runner pre-creates database `fdq` and
connects WITHOUT default-db, then issues USE fdq first (never dropping
it before connect).
"""
import pymysql, sys, re, difflib, argparse

DYN = [
    (re.compile(r"txnStartTS=\d+"), "txnStartTS=X"),
    (re.compile(r"start_ts: \d+"), "start_ts: X"),
    (re.compile(r"start_ts[=: ]\d+"), "start_ts=X"),
    (re.compile(r"2099\d{5}"), "CONNID"),
    (re.compile(r"'127\.0\.0\.1:\d+'"), "'ADDR'"),
]

def norm(s):
    for pat, rep in DYN:
        s = pat.sub(rep, s)
    return s

def connect(port):
    return pymysql.connect(host="127.0.0.1", port=port, user="root",
                           autocommit=True, charset="utf8mb4", read_timeout=120,
                           write_timeout=120)

def run_all(port, stmts, want_warn):
    out = []
    conn = connect(port)
    cur = conn.cursor()
    def ex(sql):
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            return "; ".join(repr(r)[:400] for r in rows[:50]) if rows else "(ok)"
        except Exception as e:
            return "ERR " + str(e)[:300]
    if ex("DROP DATABASE IF EXISTS fdq") == None:
        pass
    out.append(ex("DROP DATABASE IF EXISTS fdq"))
    out.append(ex("CREATE DATABASE fdq"))
    out.append(ex("USE fdq"))
    for sql in stmts:
        r = ex(sql)
        if want_warn:
            try:
                cur.execute("SHOW WARNINGS")
                w = cur.fetchall()
                if w:
                    r += " | WARN " + "; ".join(f"{a}:{b}:{c}"[:200] for a, b, c in w[:20])
            except Exception:
                pass
        out.append("=== %s\n%s" % (sql, norm(r)))
    try: conn.close()
    except Exception: pass
    return "\n".join(out)

def parse_case(path):
    stmts, buf = [], ""
    for ln in open(path, encoding="utf-8"):
        s = ln.strip()
        if not s or s.startswith("#") or s == ">>>":
            continue
        buf += (" " if buf else "") + s
        if buf.rstrip().endswith(";"):
            stmts.append(buf.rstrip().rstrip(";"))
            buf = ""
    if buf.strip():
        stmts.append(buf.strip().rstrip(";"))
    return stmts

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("casefile")
    ap.add_argument("--no-warn", action="store_true")
    a = ap.parse_args()
    stmts = parse_case(a.casefile)
    g = run_all(4000, stmts, not a.no_warn)
    r = run_all(4001, stmts, not a.no_warn)
    with open(a.casefile + ".go.txt", "w") as f: f.write(g)
    with open(a.casefile + ".rust.txt", "w") as f: f.write(r)
    if g == r:
        print("IDENTICAL (%d stmts)" % len(stmts))
    else:
        for d in difflib.unified_diff(g.splitlines(), r.splitlines(),
                                      "go", "rust", lineterm="", n=0):
            print(d)

if __name__ == "__main__":
    main()
