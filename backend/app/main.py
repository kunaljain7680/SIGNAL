from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.core.database import get_db
from app.api.endpoints import technologies

app = FastAPI(title="SIGNAL Foundation API")

@app.get("/health")
async def health_check(db: AsyncSession = Depends(get_db)):
    """
    Health check endpoint returning 200 OK.
    Requires a successful real connection to the PostgreSQL database.
    """
    try:
        await db.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Database connection failed")

app.include_router(
    technologies.router,
    prefix="/technologies",
    tags=["technologies"]
)