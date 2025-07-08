from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URI = 'sqlite:///flight_data.db'  # Update with your database URI

class DatabaseConnection:
    def __init__(self):
        self.engine = create_engine(DATABASE_URI)
        self.Session = sessionmaker(bind=self.engine)
        self.session = None

    def connect(self):
        if self.session is None:
            self.session = self.Session()

    def disconnect(self):
        if self.session is not None:
            self.session.close()
            self.session = None

    def is_connected(self):
        return self.session is not None