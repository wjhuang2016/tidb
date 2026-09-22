#!/usr/bin/env python3
"""Wire-level probes: COM_RESET_CONNECTION, multi-statement packets, ping."""
import pymysql, re, sys

def probe(port):
    out = []
    c = pymysql.connect(host="127.0.0.1", port=port, user="root", autocommit=True)
    cur = c.cursor()
    cur.execute("SET @w = 5")
    # COM_RESET_CONNECTION
    c._execute_command(14, b""); c._read_packet()
    try:
        cur.execute("SELECT @w")
        out.append("reset: @w=%r" % (cur.fetchone(),))
    except Exception as e:
        out.append("reset: ERR %s" % str(e)[:80])
    # session vars after reset
    try:
        cur.execute("SELECT @@sql_mode, @@autocommit")
        out.append("reset: vars=%r" % (cur.fetchone(),))
    except Exception as e:
        out.append("reset: vars ERR %s" % str(e)[:80])
    c.close()

    # multi-statement
    try:
        c = pymysql.connect(host="127.0.0.1", port=port, user="root",
                            client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS,
                            autocommit=True)
        cur = c.cursor()
        cur.execute("SELECT 1; SELECT 2")
        got = []
        while True:
            got.extend(cur.fetchall())
            if not cur.nextset():
                break
        out.append("multi: %r" % (got,))
        # multi with an error in the middle
        try:
            cur.execute("SELECT 3; SELECT bogus_col; SELECT 4")
            got = []
            while True:
                got.extend(cur.fetchall())
                if not cur.nextset():
                    break
            out.append("multi-err: %r" % (got,))
        except Exception as e:
            out.append("multi-err: ERR %s" % str(e)[:60])
        # error in first statement
        try:
            cur.execute("SELECT nope; SELECT 5")
            out.append("multi-first-err: %r" % (cur.fetchall(),))
        except Exception as e:
            out.append("multi-first-err: ERR %s" % str(e)[:60])
        c.close()
    except Exception as e:
        out.append("multi: CONN ERR %s" % str(e)[:80])

    # ping / reconnect
    c = pymysql.connect(host="127.0.0.1", port=port, user="root")
    c.ping(reconnect=False)
    out.append("ping: ok")
    c.close()
    return "\n".join(out)

g = probe(4000)
r = probe(4001)
if g == r:
    print("IDENTICAL")
else:
    import difflib
    for d in difflib.unified_diff(g.splitlines(), r.splitlines(), "go", "rust", lineterm=""):
        print(d)
