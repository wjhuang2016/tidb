#!/usr/bin/env python3
"""Sweep every sysvar: SET to DEFAULT / 0 / 'x' then read back. Diff Go vs Rust."""
import pymysql, sys

def run_batch(port, stmts):
    try:
        c = pymysql.connect(host="127.0.0.1", port=port, user="root", database=None,
                            charset="utf8mb4")
        cur = c.cursor()
        results = []
        for sql in stmts:
            try:
                cur.execute(sql)
                rows = cur.fetchall()
                results.append("; ".join(repr(r) for r in rows[:5]) if rows else "(ok)")
            except Exception as e:
                import re
                m = re.search(r"\((\d+),", str(e))
                results.append("ERR(%s)" % (m.group(1) if m else str(e)[:80]))
        c.close()
        return results
    except Exception as e:
        return ["CONN_ERR " + str(e)[:80]] * len(stmts)

def main():
    go = pymysql.connect(host="127.0.0.1", port=4000, user="root", database=None)
    cur = go.cursor()
    cur.execute("SHOW VARIABLES")
    names = [r[0] for r in cur.fetchall()]
    go.close()
    print("sweeping %d sysvars" % len(names), file=sys.stderr)
    total = 0
    for v in names:
        stmts = [
            ("SET SESSION %s = DEFAULT" % v, "SELECT @@SESSION.%s" % v),
            ("SET SESSION %s = 0" % v, "SELECT @@SESSION.%s" % v),
            ("SET SESSION %s = 'x'" % v, "SELECT @@SESSION.%s" % v),
        ]
        g = run_batch(4000, [s for pair in stmts for s in pair])
        r = run_batch(4001, [s for pair in stmts for s in pair])
        if g != r:
            total += 1
            print("DIVERGE %s" % v)
            for s, gv, rv in zip([x for pair in stmts for x in pair], g, r):
                if gv != rv:
                    print("  %s\n    GO  : %s\n    RUST: %s" % (s, gv[:200], rv[:200]))
    print("total divergent vars: %d" % total)

if __name__ == "__main__":
    main()
