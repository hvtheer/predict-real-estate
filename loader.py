#push data to sqlite database
import sqlite3
import pandas as pd
import csv
def loadDataToDatabase():
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    # Create the type_estate table
    cursor.execute("CREATE TABLE IF NOT EXISTS type_estate (id INTEGER PRIMARY KEY, name TEXT)")
    # Create the district table
    cursor.execute("CREATE TABLE IF NOT EXISTS district (id INTEGER PRIMARY KEY, name TEXT)")
    # Create the legal_document table
    cursor.execute("CREATE TABLE IF NOT EXISTS legal_document (id INTEGER PRIMARY KEY, name TEXT)")
    # Create the interior table
    cursor.execute("CREATE TABLE IF NOT EXISTS interior (id INTEGER PRIMARY KEY, name TEXT)")
    # Create the property table with foreign key constraints
    cursor.execute("CREATE TABLE IF NOT EXISTS property (pr_id INTEGER PRIMARY KEY, type_estate INTEGER, district INTEGER, posted_date TEXT, area REAL, price REAL, legal_document INTEGER, interior INTEGER, num_bedrooms INTEGER, num_bathrooms INTEGER, num_floors INTEGER, entrance REAL, frontage REAL, price_per_sqm REAL, FOREIGN KEY (type_estate) REFERENCES type_estate(id), FOREIGN KEY (district) REFERENCES district(id), FOREIGN KEY (legal_document) REFERENCES legal_document(id), FOREIGN KEY (interior) REFERENCES interior(id))")
    
    # Delete all data from tables
    cursor.execute("DELETE FROM property")
    cursor.execute("DELETE FROM type_estate")
    cursor.execute("DELETE FROM district")
    cursor.execute("DELETE FROM legal_document")
    cursor.execute("DELETE FROM interior")
    conn.commit()

    df_type_estate = pd.read_csv('data/results/unique_type_estate.csv')
    df_district = pd.read_csv('data/results/unique_district.csv')
    df_legal_document = pd.read_csv('data/results/unique_legal_document.csv')
    df_interior = pd.read_csv('data/results/unique_interior.csv')
    print(df_type_estate)
    print(df_district)
    print(df_legal_document)
    print(df_interior)
    for index, row in df_type_estate.iterrows():
        print(row['Key'], row['Value'])
        cursor.execute("INSERT INTO type_estate (id, name) VALUES (?, ?)", (int(row['Key']), row['Value']))
    for index, row in df_district.iterrows():
        cursor.execute("INSERT INTO district (id, name) VALUES (?, ?)", (int(row['Key']), row['Value']))
    for index, row in df_legal_document.iterrows():
        cursor.execute("INSERT INTO legal_document (id, name) VALUES (?, ?)", (int(row['Key']), row['Value']))
    for index, row in df_interior.iterrows():
        cursor.execute("INSERT INTO interior (id, name) VALUES (?, ?)", (int(row['Key']), row['Value']))
    conn.commit()

    df = pd.read_csv('data/results/standardized_data.csv')
    cursor.executemany("INSERT INTO property (pr_id, type_estate, district, posted_date, area, price, legal_document, interior, num_bedrooms, num_bathrooms, num_floors, entrance, frontage, price_per_sqm) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", df.values)
    conn.commit()
    cursor.close()
    conn.close()
