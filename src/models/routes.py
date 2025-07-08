from dataclasses import dataclass
from typing import Optional


@dataclass
class Route:
    route_id: int
    origin: str
    destination: str
    
    def validate(self) -> bool:
        if not self.origin or not self.destination:
            return False
        if self.origin == self.destination:
            return False
        return True
    
    def get_route_info(self) -> dict:
        return {
            "route_id": self.route_id,
            "origin": self.origin,
            "destination": self.destination,
        }

    
    def __repr__(self):
        return f"Route(id={self.route_id}, from: {self.origin}, to: {self.destination}, distance: {self.distance} km)"