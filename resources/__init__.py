import pathlib
from datetime import datetime
from typing import List, Literal

import tomli
from pydantic import BaseModel

Koffer = Literal["koffer1", "koffer2"]


class Timeframe(BaseModel):
    tag: str
    source: Koffer
    oldest_record: datetime
    newest_record: datetime = datetime.now()


class Household(BaseModel):
    timeframes: List[Timeframe]


path = pathlib.Path(__file__).parent / "households.toml"
with path.open(mode="rb") as fp:
    household_data = tomli.load(fp)

AVAILABLE_SENSORS: dict[str, str] = household_data["available_sensors"]
HOUSEHOLDS: dict[str, Household] = {
    household_id: Household(**household)
    for (household_id, household) in household_data["households"].items()
}
