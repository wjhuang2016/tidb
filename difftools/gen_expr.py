#!/usr/bin/env python3
"""Generate expression battery: every builtin × 10 arg shapes -> case file."""
import pymysql

c = pymysql.connect(host="127.0.0.1", port=4000, user="root")
cur = c.cursor()
cur.execute("SHOW BUILTINS")
fns = [r[0] for r in cur.fetchall()]
c.close()

shapes = [
    "()",
    "(1)",
    "('a')",
    "(NULL)",
    "(1,2)",
    "('a','b')",
    "(1.5,'a',NULL)",
    "('2020-01-01','10:20:30')",
    "(b'1',x'41')",
    "(CAST(1 AS JSON),CAST(1 AS JSON))",
]
with open("/home/wj/difftools/g-expr.txt", "w") as f:
    f.write("# expression battery: 304 builtins x 10 shapes\n")
    for fn in fns:
        for s in shapes:
            f.write(f"SELECT {fn}{s}\n")
print(f"{len(fns)} builtins x {len(shapes)} = {len(fns)*len(shapes)} probes")
