from uuid import UUID

import asyncpg

from app.domain.schemas import Invoice, ShipmentUpdate


async def create_shipment_update(
    connection: asyncpg.Connection,
    *,
    raw_event_id: UUID,
    shipment: ShipmentUpdate,
) -> UUID:
    return await connection.fetchval(
        """
        INSERT INTO shipment_updates (
            raw_event_id,
            vendor_id,
            tracking_number,
            status,
            event_timestamp
        )
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id
        """,
        raw_event_id,
        shipment.vendor_id,
        shipment.tracking_number,
        shipment.status.value,
        shipment.timestamp,
    )


async def create_invoice(
    connection: asyncpg.Connection,
    *,
    raw_event_id: UUID,
    invoice: Invoice,
) -> UUID:
    return await connection.fetchval(
        """
        INSERT INTO invoices (
            raw_event_id,
            vendor_id,
            invoice_id,
            amount,
            currency
        )
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id
        """,
        raw_event_id,
        invoice.vendor_id,
        invoice.invoice_id,
        invoice.amount,
        invoice.currency,
    )
