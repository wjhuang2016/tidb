#!/usr/bin/env python3
"""Classify divergences from paired transcripts (*.go.txt / *.rust.txt).
Buckets: VALUE (core result differs), ERRCODE (both ERR, code differs),
WARNONLY (core same, warnings differ), OTHER."""
import sys, re

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

def load(path):
    """-> {stmt: result_line}"""
    res, cur = {}, None
    for ln in open(path, encoding="utf-8", errors="replace"):
        ln = ln.rstrip("\n")
        if ln.startswith("=== "):
            cur = ln[4:]
        elif cur is not None:
            res[cur] = norm(ln)
            cur = None
    return res

def main(gopath, rustpath):
    g, r = load(gopath), load(rustpath)
    buckets = {"VALUE": [], "ERRCODE": [], "WARNONLY": [], "OTHER": []}
    for stmt in g:
        if stmt not in r:
            continue
        gl, rl = g[stmt], r[stmt]
        if gl == rl:
            continue
        gcore = gl.split(" | WARN")[0].strip()
        rcore = rl.split(" | WARN")[0].strip()
        gw = sorted(re.findall(r"(?:Warning|Error):(\d+):", gl))
        rw = sorted(re.findall(r"(?:Warning|Error):(\d+):", rl))
        if gcore != rcore:
            gerr, rerr = gcore.startswith("ERR"), rcore.startswith("ERR")
            if gerr and rerr:
                gm, rm = re.match(r"ERR \((\d+)", gcore), re.match(r"ERR \((\d+)", rcore)
                if gm and rm and gm.group(1) == rm.group(1):
                    buckets["OTHER"].append((stmt, gcore[:180], rcore[:180]))
                else:
                    buckets["ERRCODE"].append((stmt, gcore[:200], rcore[:200]))
            else:
                buckets["VALUE"].append((stmt, gcore[:200], rcore[:200]))
        elif gw != rw:
            buckets["WARNONLY"].append((stmt, ",".join(gw) or "-", ",".join(rw) or "-"))
    for k in ("VALUE", "ERRCODE", "WARNONLY", "OTHER"):
        print("### %s (%d)" % (k, len(buckets[k])))
        for stmt, a, b in buckets[k]:
            print("  %s\n    GO  : %s\n    RUST: %s" % (stmt, a, b))

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
