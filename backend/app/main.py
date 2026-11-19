from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from app.core.database import get_db
from app.api.endpoints import technologies

app = FastAPI(title="SIGNAL Foundation API")

# The Phase 1 HTML dashboard is served separately by Python's local static server.
# Only the two documented local dashboard origins are allowed.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5500",
        "http://localhost:5500",
    ],
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)

@app.get("/health")
async def health_check(db: AsyncSession = Depends(get_db)):
    """Return 200 only when the application can reach PostgreSQL."""
    try:
        await db.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception:
        raise HTTPException(status_code=500, detail="Database connection failed")

app.include_router(
    technologies.router,
    prefix="/technologies",
    tags=["technologies"],
)
