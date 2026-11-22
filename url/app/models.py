from datetime import datetime, timezone
from sqlalchemy import BigInteger, Column, DateTime, String, Text
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