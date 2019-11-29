#!/usr/bin/env python3

#python 3.6.5


import requests
import json
import base64
import sqlite3
import datetime

# https://api.exchangeratesapi.io/history?start_at=2018-01-01&end_at=2019-11-16&base=GBP

conn = sqlite3.connect('staging.db')
c = conn.cursor()
c.execute("PRAGMA foreign_keys = ON")

#initial_request = """https://api.exchangeratesapi.io/history?start_at=2018-01-01&end_at=2019-11-16&base=GBP"""
initial_request = """https://api.exchangeratesapi.io/history?start_at=2018-01-01&end_at=2019-11-16&base=GBP"""

today = datetime.datetime.now().strftime("%Y-%m-%d")
fivedaysago = (datetime.datetime.now() - datetime.timedelta(days=5)).strftime("%Y-%m-%d")

#url = initial_request
url = """https://api.exchangeratesapi.io/history?start_at=""" + fivedaysago + """&end_at=""" + today + """&base=GBP"""

print("Requesting URL: %s" % (url))

response = requests.get(url)
data = response.json()

rates = data['rates']

for day in rates.keys():
    for currency in rates[day].keys():
        print ("day %s currency %s value %s" % (day, currency, rates[day][currency]))
        c.execute("DELETE FROM exchange_rates WHERE currency = ? AND rate_date = ?", [currency, day])
        c.execute("INSERT INTO exchange_rates (currency, rate_date, gbp_value) VALUES (?,?,?)", [currency, day, float(rates[day][currency])])

conn.commit()
conn.close()
