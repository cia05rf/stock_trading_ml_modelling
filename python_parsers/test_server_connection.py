import MySQLdb
import pandas as pd
import re

db = MySQLdb.connect(host="whub49.webhostinghub.com",    # your host, usually localhost
                     user="fresha18_cia05rf",  # your username
                     passwd="h451OwNc234GoA",  # your password
                     db="fresha18_trading")    # name of the data base

# you must create a Cursor object. It will let
#  you execute all the queries you need
cur = db.cursor()

# Use all the SQL you like
cur.execute("DESCRIBE historic_prices_d")

# print all the first cell of all the rows
headers = []
for row in cur.fetchall():
    headers.append(row[0])
print(headers)

# Use all the SQL you like
cur.execute("SELECT * FROM historic_prices_d")

# print all the first cell of all the rows
data = cur.fetchall()
print(data)
data_df = pd.DataFrame(list(data),columns=headers)

print(data_df)

db.close()