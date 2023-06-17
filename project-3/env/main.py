import psycopg2 as pg
import csv
import pandas as pd
from sqlalchemy import create_engine

conn = pg.connect("host=localhost port=5434 dbname=learn user=learn password=learn")

cursor = conn.cursor()

table1 = 'latihan_users_tyo1'
table2 = 'latihan_users_tyo2'
table3 = 'latihan_users_tyo3'
table4 = 'from_file_table'

cursor.execute(f"DROP TABLE IF EXISTS {table1}")
cursor.execute(f"DROP TABLE IF EXISTS {table2}")
cursor.execute(f"DROP TABLE IF EXISTS {table3}")
cursor.execute(f"DROP TABLE IF EXISTS {table4}")
conn.commit()

cursor.execute(f'''
CREATE TABLE IF NOT EXISTS {table1}(
    id serial PRIMARY KEY,
    email text,
    name text,
    phone text,
    postal_code text    
)
''')
               
cursor.execute(f'''
CREATE TABLE IF NOT EXISTS {table2}(
    email text,
    name text,
    phone text,
    postal_code text    
)
''')

cursor.execute(f'''
CREATE TABLE IF NOT EXISTS {table3}(
    email text,
    name text,
    phone text,
    postal_code text    
)
''')
               
with open('/Users/fbramantyo/Downloads/users_w_postal_code.csv', 'r') as file :
    reader = csv.reader(file)
    next(reader)
    for row in reader:
        cursor.execute(f"INSERT INTO {table1} VALUES (default, %s, %s, %s, %s) ON CONFLICT DO NOTHING", row)             

conn.commit()
cursor.execute(f"SELECT * FROM {table1};")
users1 = cursor.fetchall()
print(f'data table {table1}:')
for user in users1:
    print(user)

print("\n")

with open('/Users/fbramantyo/Downloads/users_w_postal_code.csv', 'r') as file :
    next(file)
    cursor.copy_from(file, f'{table2}', sep=',', columns=['email','name','phone','postal_code'])

conn.commit()
cursor.execute(f"SELECT * FROM {table2};")
users2 = cursor.fetchall()
print(f'data table {table2}:')
for user in users2:
    print(user)

print("\n")

with open('/Users/fbramantyo/Downloads/users_w_postal_code.csv', 'r') as file :
    cursor.copy_expert(f"COPY public.{table3} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')", file)

conn.commit()
cursor.execute(f"SELECT * FROM {table3};")
users3 = cursor.fetchall()
print(f'data table {table3}:')
for user in users3:
    print(user)

print("\n")

df = pd.read_csv('/Users/fbramantyo/Downloads/users_w_postal_code.csv', sep = ',')
df.head()

engine = create_engine("postgresql://learn:learn@localhost:5434/learn")

df.to_sql(f'{table4}', engine)

cursor.execute(f"SELECT * FROM {table4};")
users4 = cursor.fetchall()
print(f'data table {table4}:')
for user in users3:
    print(user)
print(users4)

print("\n")

cursor.execute("SELECT * FROM information_schema.tables WHERE table_schema = 'public';")
tables = cursor.fetchall()
print("list table:")
for table in tables:
    print(table)

cursor.close()
conn.close()
