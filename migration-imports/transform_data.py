from typing import Any, Dict, Optional

PICKLIST_MAP: Dict[str, Dict[str, str]] = {
    "MarketSegmentCode__c": {
        "INDIVIDUAL": "Individual",
        "OTA": "Online Travel Agency",
        "FIT": "Free Independent Traveler",
        "LEISUREGR": "Leisure Group",
        "SPORTSGR": "Sports Group",
        "MICEGR": "MICE Group",
        "HOUSE": "House Use",
        "LCR": "Long-term Contractor Rate",
        "OTHERS": "Others",
    },

    "ReservationStatus__c": {
        "CONFIRMED": "Confirmed",
        "INHOUSE": "InHouse",
        "CHECKEDOUT": "CheckedOut",
        "CANCELED": "Canceled",
        "CANCELLED": "Canceled",   # optional safety
        "NOSHOW": "NoShow",
        "NO_SHOW": "NoShow",       # optional safety
    }
}




def map_picklist(field_api: str, value: Any, *, on_unknown: str = "none") -> Optional[str]:
    """
    on_unknown:
      - "none": return None (skip field or set null depending on your policy)
      - "keep": keep original value as string (may fail SF)
      - "raise": raise ValueError
    """

    mapping = PICKLIST_MAP.get(field_api, {})
    mapped = mapping.get(value)

    if mapped is not None:
        return mapped

    if on_unknown == "keep":
        return str(value).strip()
    if on_unknown == "raise":
        raise ValueError(f"Unknown picklist value for {field_api}: {value!r}")
    return None