import string
import random

BASE62 = string.ascii_letters + string.digits   # a-zA-Z0-9 (62 chars)


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