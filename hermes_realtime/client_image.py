"""Bounded compact client images, converted to Hermes's native vision result."""
from __future__ import annotations

import base64
import binascii
import json
from typing import Any

_FIELDS = {"type", "version", "mime_type", "data", "width", "height", "text"}


def decode_client_image(value: Any) -> Any:
    raw = value
    if isinstance(value, str):
        try:
            raw = json.loads(value)
        except (ValueError, TypeError):
            return value
    if not isinstance(raw, dict) or raw.get("type") != "client_image":
        return value
    def invalid():
        raise ValueError("Invalid compact client image")
    if (set(raw) != _FIELDS or type(raw["version"]) is not int or raw["version"] != 1
            or raw["mime_type"] != "image/jpeg" or not isinstance(raw["data"], str)
            or len(raw["data"]) > 13336 or type(raw["width"]) is not int or type(raw["height"]) is not int
            or not 1 <= raw["width"] <= 640 or not 1 <= raw["height"] <= 640
            or not isinstance(raw["text"], str) or not raw["text"].strip() or len(raw["text"]) > 512):
        invalid()
    try:
        data = base64.b64decode(raw["data"], validate=True)
    except (ValueError, binascii.Error):
        invalid()
    if (not 1 <= len(data) <= 10000 or base64.b64encode(data).decode() != raw["data"]
            or not data.startswith(b"\xff\xd8") or not data.endswith(b"\xff\xd9")):
        invalid()
    offset, dimensions = 2, None
    while offset + 4 <= len(data):
        if data[offset] != 0xff:
            invalid()
        offset += 1
        while offset < len(data) and data[offset] == 0xff:
            offset += 1
        if offset >= len(data):
            invalid()
        marker = data[offset]
        offset += 1
        if marker in (0xda, 0xd9):
            break
        if marker == 1 or 0xd0 <= marker <= 0xd7:
            continue
        size = int.from_bytes(data[offset:offset+2], "big")
        if size < 2 or offset + size > len(data):
            invalid()
        if marker in (0xc0, 0xc1, 0xc2):
            if size < 8:
                invalid()
            dimensions = (int.from_bytes(data[offset+5:offset+7], "big"), int.from_bytes(data[offset+3:offset+5], "big"))
            break
        offset += size
    if dimensions != (raw["width"], raw["height"]):
        invalid()
    return {"_multimodal": True, "text_summary": raw["text"], "content": [
        {"type": "text", "text": raw["text"]},
        {"type": "image_url", "image_url": {"url": "data:image/jpeg;base64," + raw["data"]}},
    ]}
