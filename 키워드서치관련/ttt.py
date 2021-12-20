import io
import os
import sqlite3
from collections import OrderedDict
import chardet

db_path1 = r'D:\replace\path_table.db'
db_path2 = r'D:\replace\비교대상.db'

con1 = sqlite3.connect(db_path1)
cur1 = con1.cursor()
con2 = sqlite3.connect(db_path2)
cur2 = con2.cursor()

query = "SELECT path FROM path"

cur1.execute(query)
cur2.execute(query)

c1 = []
for first in cur1.fetchall():
    c1.append(first)

c2 = []
for second in cur2.fetchall():
    c2.append(second)

result = list(set(c1) - set(c2))
print(result)


