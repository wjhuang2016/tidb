#!/usr/bin/env python3
"""Go-vs-Rust live SQL differential harness v2 — statement-at-a-time.

Runs EVERY statement (setup and probes) individually on a fresh database
per case, records each outcome, diffs the two transcripts.
Format: lines are statements; '>>>' starts the probe section (default: all
lines after >>> are probes; before: setup). '#' comments.
"""
import subprocess, sys, re, difflib

GO_PORT = 4000
RUST_PORT = 4001
MYSQL = "/usr/bin/mysql"

def run_stmt(port, sql, db, use_d=True):
    # -D is safe here: the fixture phase (no -D) guarantees dtq exists before
    # any transcript statement runs, and nothing drops dtq in-band.
    cmd = [MYSQL, "-h127.0.0.1", "-P%d" % port, "-uroot", "--batch", "--raw"]
    if use_d and db:
        cmd += ["-D", db]
    cmd += ["-e", sql]
    p = subprocess.run(cmd, capture_output=True, timeout=120)
    p.stdout = p.stdout.decode("utf-8", errors="replace")
    p.stderr = p.stderr.decode("utf-8", errors="replace")
    out, err = p.stdout.rstrip("\n"), p.stderr.rstrip("\n")
    parts = []
    if out:
        parts.append(out)
    if err:
        m = re.search(r"ERROR \d+ \(\w+\).*", err)
        parts.append("ERR: " + (m.group(0) if m else err.splitlines()[-1]))
    return "\n".join(parts) if parts else "(empty)"

def norm(t):
    # normalize environment-dependent noise
    t = re.sub(r"/tmp/gotidb-data2", "DATADIR", t)
    t = re.sub(r"/tmp/gotidb\.log", "LOGPATH", t)
    t = re.sub(r"tidb-400[01]\.sock", "SOCK", t)
    t = re.sub(r"'127\.0\.0\.1:\d+'", "'ADDR'", t)
    return t

def fixture(port):
    """Create dtq once, outside the transcript, without -D."""
    r = run_stmt(port, "DROP DATABASE IF EXISTS dtq; CREATE DATABASE dtq; USE dtq;"
                 " CREATE TABLE IF NOT EXISTS h1(a INT PRIMARY KEY, b VARCHAR(5));"
                 " INSERT IGNORE INTO h1 VALUES (1,'x')", None, use_d=False)
    if "ERROR" in r:
        print("!! FIXTURE FAILED: %s" % r[:120], file=sys.stderr)
        sys.exit(2)

def transcript(port, stmts, db):
    lines = []
    for s in stmts:
        r = norm(run_stmt(port, s, db))
        lines.append("=== %s\n%s" % (s, r))
    return "\n".join(lines)

def main(path):
    text = open(path).read()
    setup, probes, mode = [], [], "setup"
    for ln in text.splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        if ln.strip() == ">>>":
            mode = "probes"
            continue
        (setup if mode == "setup" else probes).append(ln.rstrip(";"))
    all_stmts = list(setup)
    all_stmts.extend(probes)
    fixture(GO_PORT)
    fixture(RUST_PORT)
    g = transcript(GO_PORT, all_stmts, "dtq")
    r = transcript(RUST_PORT, all_stmts, "dtq")
    if g == r:
        print("IDENTICAL (%d stmts)" % len(all_stmts))
    else:
        for d in difflib.unified_diff(g.splitlines(), r.splitlines(),
                                      "go", "rust", lineterm="", n=0):
            print(d)

if __name__ == "__main__":
    main(sys.argv[1])
