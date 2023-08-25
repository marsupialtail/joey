from trino.dbapi import connect
conn = connect(host = "ec2-35-90-164-150.us-west-2.compute.amazonaws.com", port = 8889, catalog = "hive", schema= "default", user = "hadoop")
cur = conn.cursor()
cur.execute(query)
rows = cur.fetchall()