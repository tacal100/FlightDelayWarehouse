from dataclasses import dataclass
from typing import Optional


@dataclass
class Airport:
    """Airport data model matching your normalized table structure"""
    airport_id: int
    iata_code: str
    name: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    
    def validate(self) -> bool:
        """Validate airport data"""
        if not self.iata_code or len(self.iata_code.strip()) == 0:
            return False
        if len(self.iata_code) not in [3, 4]:  # IATA (3) or ICAO (4) codes
            return False
        return True
    
    def get_airport_info(self) -> dict:
        """Get airport information as dictionary"""
        return {
            "airport_id": self.airport_id,
            "iata_code": self.iata_code,
            "name": self.name,
            "city": self.city,
            "state": self.state,
        }
    
    def __repr__(self):
        return f"Airport(code='{self.iata_code}', name='{self.name}', city='{self.city}')"