from dataclasses import dataclass
from typing import Optional


@dataclass
class Carrier:
    op_carrier: str
    carrier_name: Optional[str] = None
    
    def validate(self) -> bool:
        if not self.op_carrier or len(self.op_carrier.strip()) == 0:
            return False
        return True
    
    def get_carrier_info(self) -> dict:
        return {
            "op_carrier": self.op_carrier,
            "carrier_name": self.carrier_name,
        }
    
    def __repr__(self):
        return f"Carrier(op_carrier='{self.op_carrier}', name='{self.carrier_name}')"