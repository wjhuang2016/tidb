#!/usr/bin/env python3
"""Two-session transaction differential: same script against both servers,
one row per step with session-tagged results, then diff.

Script lines:  A: <sql>  (session A)   B: <sql>  (session B)
Prints unified diff of go-vs-rust step transcripts.
"""
import pymysql, sys, difflib

GO = dict(host="127.0.0.1", port=4000, user="root", database="txdb")
RUST = dict(host="127.0.0.1", port=4001, user="root", database="txdb")

def connect(cfg):
    return pymysql.connect(**cfg, autocommit=True)

def run_script(cfg, steps):
    conn = pymysql.connect(host=cfg["host"], port=cfg["port"], user="root",
                           database=None, autocommit=True)
    out = []
    def sess(tag):
        return conn.cursor() if tag is None else None
    # two sessions on separate connections
    conns = {"A": pymysql.connect(host=cfg["host"], port=cfg["port"], user="root",
                                  database=None, autocommit=True),
             "B": pymysql.connect(host=cfg["host"], port=cfg["port"], user="root",
                                  database=None, autocommit=True)}
    cursors = {"A": conns["A"].cursor(), "B": conns["B"].cursor()}
    for tag, sql in steps:
        try:
            cursors[tag].execute(sql)
            rows = cursors[tag].fetchall()
            res = "; ".join(repr(r) for r in rows[:20]) if rows else "(ok)"
        except Exception as e:
            res = "ERR %s" % e
        out.append("%s| %s\n   %s" % (tag, sql, res))
    for c in conns.values():
        c.close()
    conn.close()
    return "\n".join(out)

def main(path):
    steps = []
    for ln in open(path):
        ln = ln.rstrip("\n")
        if not ln.strip() or ln.lstrip().startswith("#"):
            continue
        tag, sql = ln.split(":", 1)
        steps.append((tag.strip(), sql.strip()))
    g = run_script(GO, steps)
    r = run_script(RUST, steps)
    if g == r:
        print("IDENTICAL (%d steps)" % len(steps))
    else:
        for d in difflib.unified_diff(g.splitlines(), r.splitlines(),
                                      "go", "rust", lineterm="", n=0):
            print(d)

if __name__ == "__main__":
    main(sys.argv[1])
