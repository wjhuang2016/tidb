#!/usr/bin/env python3
"""sysvar deep sweep on both servers: per var -> SET GLOBAL v='x' / SET v=DEFAULT /
SET v=0 / readback. Only prints divergent vars. Count surfaces per var."""
import pymysql, sys

def connect(port):
    return pymysql.connect(host="127.0.0.1", port=port, user="root",
                           autocommit=True, charset="utf8mb4", read_timeout=60)

def ex(cur, sql):
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        return "; ".join(repr(r)[:120] for r in rows) if rows else "(ok)"
    except Exception as e:
        return "ERR " + str(e)[:140]

def sweep(port, prefix=""):
    conn = connect(port)
    cur = conn.cursor()
    cur.execute("SHOW VARIABLES")
    names = [r[0] for r in cur.fetchall()]
    out = {}
    for n in names:
        if prefix and not n.startswith(prefix):
            continue
        # poison pills: SET GLOBAL 'x' on these kills every NEW session
        # (validate-at-use) and persists across GO restarts -> session-only.
        POISON = {"tidb_pipelined_dml_resource_policy", "tidb_cloud_storage_uri"}
        probes = [] if n in POISON else [
            "SET @@GLOBAL.%s = 'x'" % n,
            "SET @@%s = DEFAULT" % n,
            "SET @@%s = 0" % n,
            "SELECT @@%s" % n,
            "SELECT @@GLOBAL.%s" % n,
        ]
        res = []
        for sql in probes:
            try:
                res.append(ex(cur, sql))
            except Exception:
                conn = connect(port)
                cur = conn.cursor()
                res.append(ex(cur, sql))
            try:
                cur.execute("SET @@%s = DEFAULT" % n)
            except Exception:
                conn = connect(port)
                cur = conn.cursor()
                try: cur.execute("SET @@%s = DEFAULT" % n)
                except Exception: pass
        out[n] = tuple(res)
    conn.close()
    return out

import os
P = os.environ.get('SVPREFIX','')
g = sweep(4000, P)
r = sweep(4001, P)
n = 0
for k in g:
    if k in r and g[k] != r[k]:
        n += 1
        print("== %s" % k)
        for sql, a, b in zip(
            ["SET G='x'", "SET=DEF", "SET=0", "SELECT@@", "SELECT@G@"], g[k], r[k]):
            if a != b:
                print("   %s\n   GO  : %s\n   RUST: %s" % (sql, a[:140], b[:140]))
print("DIVERGENT VARS: %d" % n)
