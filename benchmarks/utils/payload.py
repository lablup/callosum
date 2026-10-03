"""
Payload generators for benchmark tests.
"""

import os
import random
import string
from typing import Any, Dict


def generate_random_payload(size_bytes: int) -> Dict[str, Any]:
    """
    Generate random payload of specified size.

    Returns a dictionary with random data that will be approximately
    the specified size when serialized with MessagePack.
    """
    # Estimate: each char is ~1 byte, plus overhead for dict structure
    # We'll generate slightly less to account for overhead
    estimated_string_size = max(1, size_bytes - 100)

    random_string = "".join(
        random.choices(string.ascii_letters + string.digits, k=estimated_string_size)
    )

    return {"data": random_string, "size": size_bytes}


def generate_compressible_payload(size_bytes: int) -> Dict[str, Any]:
    """
    Generate payload that compresses well (for compression tests).

    Returns a dictionary with repetitive data that Snappy can compress effectively.
    """
    # Use repetitive pattern that compresses well
    pattern = "ABCDEFGHIJ" * (size_bytes // 10 + 1)
    pattern = pattern[:size_bytes]

    return {"data": pattern, "size": size_bytes, "compressible": True}


def generate_incompressible_payload(size_bytes: int) -> Dict[str, Any]:
    """
    Generate payload that doesn't compress well (random data).

    Uses cryptographically random bytes which are incompressible.
    """
    random_bytes = os.urandom(size_bytes)
    # Convert to hex string for JSON serialization
    random_hex = random_bytes.hex()

    return {"data": random_hex, "size": size_bytes, "compressible": False}


def generate_structured_payload(
    size_bytes: int, num_fields: int = 10
) -> Dict[str, Any]:
    """
    Generate structured payload with multiple fields.

    Useful for testing realistic scenarios with nested data structures.
    """
    # Distribute size across fields
    field_size = max(1, (size_bytes - 200) // num_fields)

    payload: Dict[str, Any] = {
        "metadata": {"size": size_bytes, "fields": num_fields}
    }

    for i in range(num_fields):
        field_data = "".join(
            random.choices(string.ascii_letters + string.digits, k=field_size)
        )
        payload[f"field_{i}"] = field_data

    return payload


class PayloadGenerator:
    """
    Callable payload generator for benchmarks.

    Usage:
        gen = PayloadGenerator(size_bytes=1024, kind='random')
        payload = gen()  # Returns a new random payload
    """

    def __init__(
        self, size_bytes: int, kind: str = "random", structured_fields: int = 10
    ):
        """
        Initialize payload generator.

        Args:
            size_bytes: Target payload size in bytes
            kind: Type of payload - 'random', 'compressible', 'incompressible', 'structured'
            structured_fields: Number of fields for structured payloads
        """
        self.size_bytes = size_bytes
        self.kind = kind
        self.structured_fields = structured_fields

    def __call__(self) -> Dict[str, Any]:
        """Generate and return a payload."""
        if self.kind == "compressible":
            return generate_compressible_payload(self.size_bytes)
        elif self.kind == "incompressible":
            return generate_incompressible_payload(self.size_bytes)
        elif self.kind == "structured":
            return generate_structured_payload(
                self.size_bytes, self.structured_fields
            )
        else:  # random
            return generate_random_payload(self.size_bytes)
