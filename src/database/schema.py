from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Aircraft(Base):
    __tablename__ = 'aircraft'
    
    id = Column(Integer, primary_key=True)
    model = Column(String, nullable=False)
    capacity = Column(Integer, nullable=False)

class Airport(Base):
    __tablename__ = 'airports'
    
    code = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    location = Column(String, nullable=False)

class Carrier(Base):
    __tablename__ = 'carriers'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    country = Column(String, nullable=False)

class Flight(Base):
    __tablename__ = 'flights'
    
    flight_number = Column(String, primary_key=True)
    departure_time = Column(DateTime, nullable=False)
    arrival_time = Column(DateTime, nullable=False)
    aircraft_id = Column(Integer, ForeignKey('aircraft.id'))
    carrier_id = Column(Integer, ForeignKey('carriers.id'))
    
    aircraft = relationship("Aircraft")
    carrier = relationship("Carrier")

class Route(Base):
    __tablename__ = 'routes'
    
    id = Column(Integer, primary_key=True)
    origin = Column(String, ForeignKey('airports.code'))
    destination = Column(String, ForeignKey('airports.code'))
    distance = Column(Float, nullable=False)
    
    origin_airport = relationship("Airport", foreign_keys=[origin])
    destination_airport = relationship("Airport", foreign_keys=[destination])

class Weather(Base):
    __tablename__ = 'weather'
    
    id = Column(Integer, primary_key=True)
    flight_number = Column(String, ForeignKey('flights.flight_number'))
    temperature = Column(Float, nullable=False)
    humidity = Column(Float, nullable=False)
    conditions = Column(String, nullable=False)
    
    flight = relationship("Flight")