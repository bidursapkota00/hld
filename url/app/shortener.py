import hashlib
import string
import random
import threading
import time

from app.config import settings


BASE62 = string.ascii_letters + string.digits   # a-zA-Z0-9 (62 chars)
EPOCH = 1_704_067_200_000                       # 2024-01-01 UTC in ms



def encode_base62(num: int) -> str:
    """Convert a positive integer to a Base62 string."""
    if num == 0:
        return BASE62[0]

    result = []
    while num > 0:
        result.append(BASE62[num % 62])
        num //= 62

    return "".join(reversed(result))


def generate_short_code(url_id: int, length: int = 7) -> str:
    """
    Generate a short code by Base62-encoding the database ID
    and padding with random characters to reach the desired length.
    """
    base = encode_base62(url_id)

    if len(base) >= length:
        return base[:length]

    # Pad with random Base62 characters to desired length
    padding = "".join(random.choices(BASE62, k=length - len(base)))
    return base + padding

def generate_from_hash(original_url: str, length: int = 7) -> str:
    """
    Fallback: generate a short code from the URL's MD5 hash.
    Useful when you want the same URL to always produce the same code.
    """
    hash_digest = hashlib.md5(original_url.encode()).hexdigest()
    num = int(hash_digest[:12], 16)           # Use first 12 hex chars
    code = encode_base62(num)
    return code[:length]


def is_valid_alias(alias: str) -> bool:
    """Check that a custom alias contains only Base62 characters."""
    return all(c in BASE62 for c in alias) and 3 <= len(alias) <= 20


class Snowflake:
    """
    64-bit ID: 41-bit timestamp | 10-bit machine | 12-bit sequence
    means 4096 IDs per ms per machine, no collisions, no DB coordination.
    """

    def __init__(self, machine_id: int = 0):
        assert 0 <= machine_id < 1024, "machine_id must be between 0 and 1023"
        self.machine_id = machine_id
        self._seq = 0
        self._last_ts = -1
        self._lock = threading.Lock()

    def next_id(self) -> int:
        with self._lock:
            ts = int(time.time() * 1000) - EPOCH
            if ts == self._last_ts:
                self._seq = (self._seq + 1) & 0xFFF
                if self._seq == 0:              # 4096 exhausted, wait 1 ms
                    while ts <= self._last_ts:
                        ts = int(time.time() * 1000) - EPOCH
            else:
                self._seq = 0
            self._last_ts = ts
            return (ts << 22) | (self.machine_id << 12) | self._seq


# Singleton instance per worker process
snowflake = Snowflake(machine_id=settings.machine_id)


def generate_snowflake_code() -> str:
    """Generate an ~11-character unique, time-sortable short code in memory."""
    return encode_base62(snowflake.next_id())