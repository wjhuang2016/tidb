#!/usr/bin/env python3
"""Two-session battery: locks, kill, cross-session visibility."""
import pymysql, sys, re

def connect(port):
    return pymysql.connect(host="127.0.0.1", port=port, user="root",
                           autocommit=True, charset="utf8mb4")

def ex(cur, sql):
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        return "; ".join(repr(r)[:120] for r in rows) if rows else "(ok)"
    except Exception as e:
        return "ERR " + str(e)[:130]

def run(port):
    out = []
    a, b = connect(port), connect(port)
    ca, cb = a.cursor(), b.cursor()
    ca.execute("DROP DATABASE IF EXISTS msq"); ca.execute("CREATE DATABASE msq"); ca.execute("USE msq")
    ca.execute("CREATE TABLE ms(a INT PRIMARY KEY)")
    cb.execute("USE msq"); ca.execute("INSERT INTO ms VALUES (1),(2)")
    out.append(ex(ca, "SELECT GET_LOCK('lk1', 1)"))
    out.append(ex(cb, "SELECT IS_USED_LOCK('lk1') IS NOT NULL, GET_LOCK('lk1', 0)"))
    out.append(ex(ca, "SELECT RELEASE_LOCK('lk1')"))
    out.append(ex(cb, "SELECT GET_LOCK('lk1', 0)"))
    out.append(ex(cb, "SELECT RELEASE_LOCK('lk1')"))
    out.append(ex(cb, "SELECT RELEASE_LOCK('no_such_lk')"))
    out.append(ex(ca, "SELECT IS_FREE_LOCK('lk2'), IS_FREE_LOCK(NULL)"))
    out.append(ex(ca, "SELECT GET_LOCK('lk2', 0)"))
    out.append(ex(ca, "SELECT GET_LOCK('lk2', 0)"))
    out.append(ex(ca, "SELECT RELEASE_ALL_LOCKS()"))
    # transaction visibility
    ca.execute("SET autocommit=0"); cb.execute("SET autocommit=0")
    ca.execute("INSERT INTO ms VALUES (10)")
    out.append(ex(cb, "SELECT COUNT(*) FROM ms"))
    ca.execute("COMMIT")
    out.append(ex(cb, "SELECT COUNT(*) FROM ms"))
    ca.execute("SET autocommit=1"); cb.execute("SET autocommit=1")
    # lock wait: b holds row lock, a tries update with small timeout
    cb.execute("BEGIN"); cb.execute("SELECT * FROM ms WHERE a = 1 FOR UPDATE")
    ca.execute("SET innodb_lock_wait_timeout = 1")
    out.append(ex(ca, "UPDATE ms SET a = 1 WHERE a = 1"))
    out.append(ex(cb, "COMMIT"))
    out.append(ex(ca, "UPDATE ms SET a = 1 WHERE a = 1"))
    # KILL QUERY on self is an error; kill bogus id
    out.append(ex(ca, "KILL 4294967295"))
    out.append(ex(ca, "KILL QUERY 4294967295"))
    out.append(ex(ca, "SHOW PROCESSLIST"))
    out.append(ex(cb, "SELECT CONNECTION_ID() IS NOT NULL"))
    a.close(); b.close()
    return "\n".join(out)

g = run(4000); r = run(4001)
if g == r:
    print("MULTISESSION IDENTICAL")
else:
    for i, (x, y) in enumerate(zip(g.splitlines(), r.splitlines()), 1):
        if x != y:
            print(f"[{i}]\n  GO  : {x}\n  RUST: {y}")
