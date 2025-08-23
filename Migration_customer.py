import csv
import pandas as pd
from sqlalchemy import create_engine, Table, Column, String, MetaData
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert

DATABASE_URL = 'postgresql+psycopg2://postgres:alicedatabase@localhost:5543/datamigrering'

metadata = MetaData()

customers = Table(
    'customers', metadata,
    Column('customer_id', String),  # Gör denna nullable eller sätt autogenerering i databasen
    Column('customer', String),
    Column('address', String),
    Column('phone', String),
    Column('personnummer', String, unique=True),
)

engine = create_engine(DATABASE_URL)

csv_file_path = 'kunder_utan_account.csv'  # Justerad filväg

connection = engine.connect()
transaction = connection.begin()

try:
    with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)

        for row in reader:
            insert_stmt = insert(customers).values(
                customer=row['Customer'],
                address=row['Address'],
                phone=row['Phone'],
                personnummer=row['Personnummer']
            )

            do_nothing_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['personnummer'])
            connection.execute(do_nothing_stmt)

    transaction.commit()
    print("All records inserted successfully (duplicates skipped).")

except (SQLAlchemyError, IOError) as e:
    print(f"Error occurred: {e}")
    transaction.rollback()
finally:
    connection.close()


