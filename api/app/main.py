from fastapi import FastAPI, HTTPException, Query
from datetime import datetime, timedelta
from pydantic import BaseModel
import uvicorn

app = FastAPI(title="FX Rate API", description="API for accessing foreign exchange rates")

# Models
class HealthCheck(BaseModel):
    status: str
    timestamp: datetime

@app.get("/", response_model=dict)
async def root():
    return {
        "status": "success",
        "message": "Welcome to FX Rate API",
    }

# Health check endpoint
@app.get("/health", response_model=HealthCheck)
async def health_check():
    """Health check endpoint"""
    try:
        return HealthCheck(
            status="healthy",
            timestamp=datetime.now()
        )
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Service unhealthy: {str(e)}"
        )

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
