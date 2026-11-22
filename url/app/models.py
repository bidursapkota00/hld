from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger, Boolean, Column, DateTime, Index, String, Text
)

from app.database import Base


class URL(Base):
    __tablename__ = "urls"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    short_code = Column(String(16), unique=True, nullable=False, index=True)
    original_url = Column(Text, nullable=False)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    expires_at = Column(DateTime(timezone=True), nullable=True)
    is_active = Column(Boolean, default=True)

    __table_args__ = (
        Index("idx_expires_at", "expires_at"),
    )