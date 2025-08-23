import csv
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, ForeignKey, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError  # <--- Viktigt importera här

DATABASE_URL = 'postgresql+psycopg2://postgres:alicedatabase@localhost:5543/datamigrering'

metadata = MetaData()

customers = Table(
    'customers', metadata,
    Column('id', Integer, primary_key=True),
    Column('customer', String)
)

accounts = Table(
    'accounts', metadata,
    Column('account_number', String, primary_key=True),
    Column('customer_id', Integer, ForeignKey('customers.id'), nullable=False)
)

engine = create_engine(DATABASE_URL)
connection = engine.connect()
transaction = connection.begin()

csv_file_path = 'accounts.csv'

try:
    with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            customer_id_result = connection.execute(
                select(customers.c.id).where(customers.c.customer == row['Customer'])
            ).first()

            if not customer_id_result:
                print(f"Kund {row['Customer']} finns inte i databasen, hoppar över.")
                continue

            customer_id = customer_id_result[0]

            insert_stmt = insert(accounts).values(
                account_number=row['BankAccount'],
                customer_id=customer_id
            )

            do_nothing_stmt = insert_stmt.on_conflict_do_nothing(index_elements=['account_number'])
            connection.execute(do_nothing_stmt)

            print(f"Konto {row['BankAccount']} kopplat till kund {row['Customer']} importerades.")

    transaction.commit()
    print("Import av konton klar!")

except (SQLAlchemyError, IOError) as e:
    print(f"Fel vid import: {e}")
    transaction.rollback()
finally:
    connection.close()
