#!/usr/bin/env python3

#python 3.6.5


import sqlite3
import datetime
import csv

conn = sqlite3.connect('staging.db')
c = conn.cursor()
c.execute("PRAGMA foreign_keys = ON")

input_file = 'ad_affiliations.csv'

timestamp = datetime.datetime.now()

c.execute("DELETE FROM funding_entity_affiliations")

print("Loading CSV: %s" % (csv))

with open(input_file) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0

    for row in csv_reader:
        if line_count == 0:
            print(f'Column names are {", ".join(row)}')
        else:
            data = [None if v is '' else v for v in row[1:4]]
            print(f'Inserting row %s' % (",".join(['NULL' if v is None else v for v in data])))
            c.execute("INSERT INTO funding_entity_affiliations (funding_entity, affiliation_party, affiliation_brexit, date_added) VALUES (?,?,?,?)",
                data + [timestamp])
        line_count+=1
    print(f'Processed {line_count} lines.')

conn.commit()
conn.close()
