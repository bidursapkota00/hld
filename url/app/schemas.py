from datetime import datetime
from pydantic import BaseModel, ConfigDict, HttpUrl


class URLCreate(BaseModel):
    url: HttpUrl                              # Validates that input is a valid URL
    custom_alias: str | None = None           # Optional custom short code
    expires_in_days: int | None = None        # Optional TTL in days

class URLResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)   # Allow ORM model to Pydantic conversion

    short_code: str
    short_url: str                            # Full shortened URL
    original_url: str
    created_at: datetime
    expires_at: datetime | None