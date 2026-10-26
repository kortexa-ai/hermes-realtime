"""Verify the API's scoped client-tool admission without trusting room metadata."""
from __future__ import annotations

import hashlib
import hmac
import json
import re
import time
from dataclasses import dataclass
from typing import Any

ADMISSION_KEY = "kortexa_voice_client"
MAX_ADMISSION_BYTES = 16 * 1024
ADMISSION_TTL = 3600


@dataclass(frozen=True)
class VoiceContext:
    base: str
    display: str


@dataclass(frozen=True)
class ClientAdmission:
    room: str
    participant: str
    principal: str
    voice: VoiceContext


def parse_voice_context(value: Any) -> VoiceContext | None:
    if not isinstance(value, dict) or set(value) != {"version", "base", "display"}:
        return None
    if type(value["version"]) is not int or value["version"] != 1:
        return None
    try:
        if not all(isinstance(value[k], str) and 0 < len(value[k].encode("utf-8")) <= 2048 for k in ("base", "display")):
            return None
    except UnicodeError:
        return None
    return VoiceContext(value["base"], value["display"])


def admission_payload(value: dict[str, Any]) -> str:
    digest = lambda text: hashlib.sha256(text.encode("utf-8")).hexdigest()
    return "\n".join([
        "kortexa.voice-client.v1", value["room"], value["participant"], value["principal"],
        str(value["issued_at"]), str(value["expires_at"]), value["nonce"],
        digest(value["voice"]["base"]), digest(value["voice"]["display"]),
    ])


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate admission field")
        result[key] = value
    return result


def verify_client_admission(metadata: Any, *, room: str, participant: str, key: str,
                            now: float | None = None) -> ClientAdmission | None:
    if not key or not isinstance(metadata, str):
        return None
    try:
        if len(metadata.encode("utf-8")) > MAX_ADMISSION_BYTES:
            return None
        outer = json.loads(metadata, object_pairs_hook=_unique_object)
        value = outer.get(ADMISSION_KEY) if isinstance(outer, dict) else None
        fields = {"version", "room", "participant", "principal", "issued_at", "expires_at", "nonce", "voice", "signature"}
        if not isinstance(value, dict) or set(value) != fields:
            return None
        if type(value["version"]) is not int or value["version"] != 1:
            return None
        if value["room"] != room or value["participant"] != participant:
            return None
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", room) or not isinstance(participant, str) or not 0 < len(participant.encode("utf-8")) <= 128:
            return None
        if not all(isinstance(value[k], str) and re.fullmatch(r"[0-9a-f]{64}", value[k]) for k in ("principal", "signature")):
            return None
        if not isinstance(value["nonce"], str) or not re.fullmatch(r"[0-9a-f-]{36}", value["nonce"]):
            return None
        issued, expires = value["issued_at"], value["expires_at"]
        clock = time.time() if now is None else now
        if type(issued) is not int or type(expires) is not int or not 0 <= issued <= clock + 30 or not clock < expires <= issued + ADMISSION_TTL:
            return None
        voice = parse_voice_context(value["voice"])
        if voice is None:
            return None
        expected = hmac.new(key.encode("utf-8"), admission_payload(value).encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(value["signature"], expected):
            return None
        return ClientAdmission(room, participant, value["principal"], voice)
    except (UnicodeError, ValueError, TypeError, KeyError, RecursionError):
        return None
