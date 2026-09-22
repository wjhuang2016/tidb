#!/usr/bin/env python3
"""Go-vs-Rust live SQL differential harness.

Usage: diffq.py <case-file>
Case file format: '#' comments, '>>>' separates setup from probes.
Each non-comment line after >>> is one probe; each is run on a FRESH
connection after replaying setup, capturing:
  result rows / affected / error code+message / warning count+text
Outputs a unified diff of normalized Go vs Rust transcripts.
"""
import subprocess, sys, re, difflib

GO_PORT = 4000
RUST_PORT = 4001
MYSQL = "/usr/bin/mysql"

def run(port, sql, db=None):
    cmd = [MYSQL, "-h127.0.0.1", "-P%d" % port, "-uroot",
           "--batch", "--raw", "--comments", "-D", db or "test",
           "-e", sql + "\nSHOW WARNINGS;"]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    out = p.stdout.strip()
    err = p.stderr.strip()
    # normalize: mysql cli prints errors to stderr as "ERROR <code> (state): msg"
    return out, err

def normalize(text, port):
    t = text
    t = re.sub(r"v[0-9]+\.[0-9]+\.[0-9]+[-\w.]*", "vX", t)
    t = re.sub(r"server version.*", "server version X", t)
    t = re.sub(r"\b\d+\.\d+\.\d+-[a-z0-9]+", "ver", t)
    return t

def main(path):
    lines = open(path).read().splitlines()
    setup, probes, mode = [], [], "setup"
    for ln in lines:
        if ln.startswith("#") or not ln.strip():
            continue
        if ln.strip() == ">>>":
            mode = "probes"
            continue
        (setup if mode == "setup" else probes).append(ln)

    setup_sql = "; ".join(setup)
    go_t, rust_t = [], []
    for probe in probes:
        # wrap: drop db, recreate setup each time for isolation
        pre = "DROP DATABASE IF EXISTS dt; CREATE DATABASE dt; USE dt; " + setup_sql
        for name, port, acc in (("GO", GO_PORT, go_t), ("RUST", RUST_PORT, rust_t)):
            out, err = run(port, pre + "; " + probe)
            acc.append("=== %s" % probe)
            if out:
                acc.append(out)
            if err:
                acc.append("ERR: " + err.replace("\n", " | "))
    g = "\n".join(go_t)
    r = "\n".join(rust_t)
    if g == r:
        print("IDENTICAL (%d probes)" % len(probes))
    else:
        for d in difflib.unified_diff(g.splitlines(), r.splitlines(),
                                      "go", "rust", lineterm="", n=1):
            print(d)

if __name__ == "__main__":
    main(sys.argv[1])
