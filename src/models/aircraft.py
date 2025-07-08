from dataclasses import dataclass
from typing import Optional


@dataclass
class Aircraft:
    tail_num: str
    manufacturer: Optional[str] = None
    icao_type: Optional[str] = None
    year_of_manufacture: Optional[int] = None
    op_carrier: Optional[str] = None
    
    def validate(self) -> bool:
        if not self.tail_num or len(self.tail_num.strip()) == 0:
            return False
        if self.year_of_manufacture and (self.year_of_manufacture < 1900 or self.year_of_manufacture > 2024):
            return False
        return True
    
    def get_aircraft_info(self) -> dict:
        return {
            "tail_num": self.tail_num,
            "manufacturer": self.manufacturer,
            "icao_type": self.icao_type,
            "year_of_manufacture": self.year_of_manufacture,
            "op_carrier": self.op_carrier
        }
    
    def __repr__(self):
        return f"Aircraft(tail_num='{self.tail_num}', manufacturer='{self.manufacturer}', icao_type='{self.icao_type}')"