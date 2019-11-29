#!/usr/bin/env python3

#python 3.6.5


import sqlite3
import datetime
import csv

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

conn = sqlite3.connect('staging.db')
conn.row_factory = dict_factory
c = conn.cursor()
c.execute("PRAGMA foreign_keys = ON")

output_file = 'ad_funding_entities.csv'

print("Writing CSV: %s" % (output_file))

c.execute("""SELECT SUM(A.spend_upper) AS spend, A.funding_entity, B.affiliation_party, B.affiliation_brexit
    FROM ads A
    LEFT OUTER JOIN funding_entity_affiliations B ON A.funding_entity = B.funding_entity
    GROUP BY A.funding_entity ORDER BY spend ASC""")
result = c.fetchall()
columns = ['spend','funding_entity','affiliation_party','affiliation_brexit']

with open(output_file, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(columns) # header row
    rows = 0
    for row in result:
        data = []
        for col in columns:
            data += [row[col]]
        writer.writerow(data)
        rows += 1

print("Wrote %d rows" % (rows))

conn.commit()
conn.close()
