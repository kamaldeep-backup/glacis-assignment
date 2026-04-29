from datetime import datetime
from enum import StrEnum
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ShipmentStatus(StrEnum):
    TRANSIT = "TRANSIT"
    DELIVERED = "DELIVERED"
    EXCEPTION = "EXCEPTION"


class StrictBaseModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class ShipmentUpdate(StrictBaseModel):
    event_type: Literal["SHIPMENT_UPDATE"]
    vendor_id: str = Field(min_length=1)
    tracking_number: str = Field(min_length=1)
    status: ShipmentStatus
    timestamp: datetime


class Invoice(StrictBaseModel):
    event_type: Literal["INVOICE"]
    vendor_id: str = Field(min_length=1)
    invoice_id: str = Field(min_length=1)
    amount: float = Field(ge=0)
    currency: str = Field(min_length=3, max_length=3)

    @field_validator("currency")
    @classmethod
    def normalize_currency(cls, value: str) -> str:
        return value.upper()


class Unclassified(StrictBaseModel):
    event_type: Literal["UNCLASSIFIED"]
    reason: str = Field(min_length=1)


NormalizedWebhook = Annotated[
    Union[ShipmentUpdate, Invoice, Unclassified],
    Field(discriminator="event_type"),
]
