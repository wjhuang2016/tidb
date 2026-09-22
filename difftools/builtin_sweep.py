#!/usr/bin/env python3
"""Sweep every builtin with generic arg shapes, diff Go vs Rust per statement.
Only records DIVERGENT statements."""
import pymysql, sys, re

PROBES = [
    ("() ", None),  # placeholder replaced below
]

def probes_for(fn):
    return [
        f"SELECT {fn}()",
        f"SELECT {fn}(1)",
        f"SELECT {fn}('a')",
        f"SELECT {fn}(1,2)",
        f"SELECT {fn}('a','b')",
        f"SELECT {fn}(1.5,'a',NULL)",
    ]

def run(port, sql):
    try:
        c = pymysql.connect(host="127.0.0.1", port=port, user="root", database=None,
                            charset="utf8mb4")
        cur = c.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
            return "; ".join(repr(r) for r in rows[:10])
        except Exception as e:
            m = re.search(r"\((\d+),", str(e))
            return "ERR(%s)" % (m.group(1) if m else str(e)[:60])
        finally:
            c.close()
    except Exception as e:
        return "CONN_ERR " + str(e)[:60]

def main():
    go = pymysql.connect(host="127.0.0.1", port=4000, user="root", database=None)
    cur = go.cursor()
    cur.execute("SHOW BUILTINS")
    fns = [r[0] for r in cur.fetchall()]
    go.close()
    print("scanning %d builtins x %d probes" % (len(fns), 6), file=sys.stderr)
    total = 0
    for fn in fns:
        for sql in probes_for(fn):
            g = run(4000, sql)
            r = run(4001, sql)
            if g != r:
                total += 1
                print("DIVERGE %s\n  GO  : %s\n  RUST: %s" % (sql, g[:300], r[:300]))
    print("total divergent probes: %d" % total)

if __name__ == "__main__":
    main()
