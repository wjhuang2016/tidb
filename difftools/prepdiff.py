#!/usr/bin/env python3
"""Prepared-statement (binary protocol) battery via mysql-connector if present, else pymysql text-protocol PREPARE.
Runs same statements against both servers, prints diffs only."""
import sys
try:
    import mysql.connector as mc
except ImportError:
    print("SKIP: mysql-connector-python not installed")
    sys.exit(0)

STMTS = [
    "SELECT ?",
    "SELECT ? + ?",
    "SELECT ? FROM fdq.pd WHERE a = ?",
    "INSERT INTO fdq.pd (a, b) VALUES (?, ?)",
    "SELECT a FROM fdq.pd WHERE b LIKE ?",
    "SELECT ? FROM fdq.pd",
    "SELECT NULLIF(?, ?)",
]

def run(port):
    out = []
    try:
        conn = mc.connect(host="127.0.0.1", port=port, user="root")
    except Exception as e:
        return "CONN_FAIL " + str(e)[:100]
    cur = conn.cursor(prepared=True)
    def ex(sql, args=()):
        try:
            cur.execute(sql, args)
            rows = cur.fetchall()
            return "; ".join(repr(r)[:120] for r in rows) if rows else "(ok)"
        except Exception as e:
            return "ERR " + str(e)[:150]
    ex("DROP DATABASE IF EXISTS fdq"); ex("CREATE DATABASE fdq"); ex("USE fdq")
    ex("CREATE TABLE pd(a INT PRIMARY KEY, b VARCHAR(5))")
    out.append(ex(STMTS[0], (42,)))
    out.append(ex(STMTS[1], (1, 2)))
    out.append(ex(STMTS[1], ('a', 'b')))
    out.append(ex(STMTS[1], (1.5, None)))
    out.append(ex(STMTS[3], (1, 'x')))
    out.append(ex(STMTS[2], (1,)))
    out.append(ex(STMTS[4], ('x%',)))
    out.append(ex(STMTS[5], (None,)))
    out.append(ex(STMTS[6], (1, 2)))
    out.append(ex("DEALLOCATE PREPARE no_such_stmt"))
    # re-prepare after schema change
    out.append(ex("ALTER TABLE pd ADD COLUMN c INT"))
    out.append(ex(STMTS[2], (1,)))
    out.append(ex("SELECT ? = ?", (b'bytes', 'bytes')))
    conn.close()
    return "\n".join(out)

g = run(4000)
r = run(4001)
if g == r:
    print("PREPARED IDENTICAL")
else:
    for i, (a, b) in enumerate(zip(g.splitlines(), r.splitlines()), 1):
        if a != b:
            print(f"[{i}]\n  GO  : {a}\n  RUST: {b}")
