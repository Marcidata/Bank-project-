from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Numeric
from sqlalchemy.orm import declarative_base, relationship
from webcolors import names



Base = declarative_base()

class Customer(Base):
    __tablename__ = 'customers'
    id = Column(Integer, primary_key=True, autoincrement=True)
    customer = Column(String, nullable=False)
    address = Column(String)
    phone = Column(String)
    personnummer = Column(String, unique=True)

    accounts = relationship('Account', back_populates='customer')

    def __init__(self, customer, address, phone, personnummer):
        self.customer = customer
        self.address = address
        self.phone = phone
        self.personnummer = personnummer
        self.accounts = []

    def __repr__(self):
        return f'Customer({self.customer}, {self.address}, {self.phone}, {self.personnummer})'

    def add_account(self, account_number, customer_id):
        self.accounts.append(account_number)
        new_account = (Account(account_number=account_number, customer_id=customer_id))
        return new_account




class Account(Base):
    __tablename__ = 'accounts'

    account_number = Column(String, primary_key=True)
    customer_id = Column(Integer, ForeignKey('customers.id'), nullable=False)

    # Relationships
    customer = relationship('Customer', back_populates='accounts')

    sent_transactions = relationship(
        'Transaction',
        foreign_keys='Transaction.sender_account',
        back_populates='sender_account_rel'
    )
    received_transactions = relationship(
        'Transaction',
        foreign_keys='Transaction.receiver_account',
        back_populates='receiver_account_rel'
    )


    def __init__(self, account_number, customer_id):
        self.account_number = account_number
        self.customer_id = customer_id

    def __repr__(self):
        return f'{self.account_number} has balance of {self.balance}'



class Transaction(Base):
    __tablename__ = 'transactions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(String, unique=True)
    timestamp = Column(DateTime)
    amount = Column(Float, nullable=False)
    currency = Column(String)
    sender_account = Column(String, ForeignKey('accounts.account_number'), nullable=False)
    receiver_account = Column(String, ForeignKey('accounts.account_number'), nullable=False)
    sender_country = Column(String, nullable=True)
    sender_municipality = Column(String, nullable=True)
    receiver_country = Column(String, nullable=True)
    receiver_municipality = Column(String, nullable=True)
    transaction_type = Column(String, nullable=True)
    notes = Column(String, nullable=True)

    sender_account_rel = relationship(
        'Account',
        foreign_keys=[sender_account],
        back_populates='sent_transactions'
    )
    receiver_account_rel = relationship(
        'Account',
        foreign_keys=[receiver_account],
        back_populates='received_transactions'
    )

    def __init__(self, id, timestamp, amount, currency, sender_account, receiver_account, sender_country,
                 sender_municipality, receiver_country, receiver_municipality, transaction_type, notes):
        self.id = id
        self.timestamp = timestamp
        self.amount = amount
        self.currency = currency
        self.sender_account = sender_account
        self.receiver_account = receiver_account
        self.sender_country = sender_country
        self.sender_municipality = sender_municipality
        self.receiver_country = receiver_country
        self.receiver_municipality = receiver_municipality
        self.transaction_type = transaction_type
        self.notes = notes

    def __repr__(self):
        return f'Transaction {self.id} of {self.amount} {self.currency} from {self.sender_account} to {self.receiver_account} occurred at {self.timestamp}'