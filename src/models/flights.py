from dataclasses import dataclass
from datetime import datetime, time
from typing import Optional


@dataclass
class Flight:
    """Flight data model matching your normalized table structure"""
    flight_id: int
    fl_date: datetime
    dep_hour: int
    sched_dep_time: Optional[time] = None
    dep_delay: Optional[int] = None
    cancelled: bool = False
    tail_num: Optional[str] = None
    route_id: Optional[int] = None
    weather_id: Optional[int] = None  
    
    def validate(self) -> bool:
        """Validate flight data"""
        if not self.fl_date:
            return False
        if self.dep_hour < 0 or self.dep_hour > 23:
            return False
        if self.dep_delay and self.dep_delay < -60:  # No departure more than 1 hour early
            return False
        return True
    
    def get_flight_info(self) -> dict:
        """Get flight information as dictionary"""
        return {
            "flight_id": self.flight_id,
            "fl_date": self.fl_date,
            "dep_hour": self.dep_hour,
            "sched_dep_time": self.sched_dep_time,
            "dep_delay": self.dep_delay,
            "cancelled": self.cancelled,
            "tail_num": self.tail_num,
            "route_id": self.route_id,
            "weather_id": self.weather_id
        }
    
    def is_delayed(self) -> bool:
        return self.dep_delay and self.dep_delay > 0
    
    def is_on_time(self) -> bool:
        return self.dep_delay is not None and -15 <= self.dep_delay <= 15
    
    #If I want to categorize delays
    def delay_category(self) -> str:
        if not self.dep_delay:
            return "Unknown"
        if self.dep_delay <= 0:
            return "Early/On-time"
        elif self.dep_delay <= 15:
            return "Minor delay"
        elif self.dep_delay <= 60:
            return "Moderate delay"
        else:
            return "Major delay"
        
    def is_cancelled(self) -> bool:
        return self.cancelled
    
    def __repr__(self):
        return f"Flight(id={self.flight_id}, date={self.fl_date.date()}, hour={self.dep_hour}, delay={self.dep_delay})"