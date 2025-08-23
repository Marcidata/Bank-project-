import csv
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, Float, ForeignKey, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError

DATABASE_URL = 'postgresql+psycopg2://postgres:alicedatabase@localhost:5543/datamigrering'

Base = declarative_base()

class Account(Base):
    __tablename__ = 'accounts'
    id = Column(Integer, primary_key=True, autoincrement=True)
    account_number = Column(String, unique=True, nullable=False)

class Transaction(Base):
    __tablename__ = 'transactions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(String, unique=True)
    timestamp = Column(DateTime)
    amount = Column(Float, nullable=False)
    currency = Column(String)
    sender_account = Column(String, ForeignKey('accounts.account_number'), nullable=False)
    receiver_account = Column(String, ForeignKey('accounts.account_number'), nullable=False)
    sender_country = Column(String)
    sender_municipality = Column(String)
    receiver_country = Column(String)
    receiver_municipality = Column(String)
    transaction_type = Column(String)
    notes = Column(String)

def parse_datetime(dt_str):
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def get_valid_accounts(session):
    accounts = session.query(Account.account_number).all()
    return set(acc[0] for acc in accounts)

def import_transactions(csv_file_path, session):
    try:
        valid_accounts = get_valid_accounts(session)
        print(f"🔍 Found {len(valid_accounts)} valid accounts in database.")

        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            skipped = 0
            added = 0

            for row in reader:
                sender = row.get('sender_account')
                receiver = row.get('receiver_account')

                if sender not in valid_accounts or receiver not in valid_accounts:
                    skipped += 1
                    print(f"⛔️ Skipping transaction {row.get('transaction_id')} (invalid account)")
                    continue

                transaction = Transaction(
                    transaction_id=row.get('transaction_id'),
                    timestamp=parse_datetime(row.get('timestamp')),
                    amount=float(row.get('amount', 0)),
                    currency=row.get('currency'),
                    sender_account=sender,
                    receiver_account=receiver,
                    sender_country=row.get('sender_country'),
                    sender_municipality=row.get('sender_municipality'),
                    receiver_country=row.get('receiver_country'),
                    receiver_municipality=row.get('receiver_municipality'),
                    transaction_type=row.get('transaction_type'),
                    notes=row.get('notes')
                )
                session.add(transaction)
                added += 1

            session.commit()
            print(f"✅ {added} transactions imported successfully. ❌ {skipped} skipped due to invalid accounts.")

    except (SQLAlchemyError, IOError) as e:
        session.rollback()
        print(f"❗ Error occurred during import, rolled back. Details: {e}")

if __name__ == "__main__":
    engine = create_engine(DATABASE_URL)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    csv_file_path = '/Users/AliceNguyen/PycharmProjects/Datamigrering-demo/valid_transactions.csv'
    import_transactions(csv_file_path, session)

    session.close()
