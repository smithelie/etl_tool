import sqlite3
import random

# Define some sample names and countries
names = ['Alice', 'Bob', 'Charlie', 'David', 'Eva', 'Fiona', 'George', 'Hannah', 'Ian', 'Julia', 'Kyle', 'Liam', 'Mia',
         'Nora', 'Oliver', 'Penelope', 'Quinn', 'Rachel', 'Steve', 'Tina']
countries = ['USA', 'Canada', 'UK', 'Australia', 'Germany', 'France', 'Spain', 'Italy', 'Brazil', 'Argentina']

# Establish a connection to the SQLite database
database = './data/database.db'
conn = sqlite3.connect(database)
cursor = conn.cursor()

# Create the table if it does not exist
create_ddl = 'CREATE TABLE IF NOT EXISTS people (name TEXT, age INTEGER, country TEXT)'
cursor.execute(create_ddl)

# Prepare to insert data
insert_ddl = f'INSERT INTO people (name, age, country) VALUES (?, ?, ?)'

# Generate random data and insert it into the table
for index in range(20):
    name = random.choice(names)
    age = random.randint(20, 80)
    country = random.choice(countries)
    cursor.execute(insert_ddl, (name, age, country))

# Commit the changes and close the connection
conn.commit()
conn.close()
