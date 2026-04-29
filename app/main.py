from fastapi import FastAPI


app = FastAPI(title="AI Webhook Ingestion Service")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/webhooks", status_code=202)
async def accept_webhook(payload: dict) -> dict[str, object]:
    return {"status": "RECEIVED", "payload": payload}
