from dataclasses import dataclass
from typing import Optional


@dataclass
class Weather:
    weather_id: int
    wind_speed: Optional[float] = None
    temperature: Optional[float] = None
    active_weather: Optional[str] = None
    visibility: Optional[float] = None
    
    def validate(self) -> bool:
        if self.temperature and (self.temperature < -100 or self.temperature > 60):
            return False
        if self.wind_speed and self.wind_speed < 0:
            return False
        if self.visibility and self.visibility < 0:
            return False
        return True
    
    def get_weather_info(self) -> dict:
        return {
            "weather_id": self.weather_id,
            "wind_speed": self.wind_speed,
            "temperature": self.temperature,
            "active_weather": self.active_weather,
            "visibility": self.visibility
        }
    
    def is_severe_weather(self) -> bool:
        #TODO Rework this method to check for severe weather conditions
        """Check if weather conditions are severe"""
        severe_conditions = ["storm", "snow", "rain", "fog", "tornado", "hurricane"]
        if self.active_weather:
            return any(condition in self.active_weather.lower() for condition in severe_conditions)
        return False
    
    def __repr__(self):
        return f"Weather(id={self.weather_id}, temp={self.temperature}°, conditions='{self.active_weather}')"