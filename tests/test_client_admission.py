"""The API grant is scoped, signed and independent of a client's instructions."""
from __future__ import annotations

import copy
import hashlib
import hmac
import json

import pytest

from hermes_realtime.client_admission import (
    ADMISSION_KEY, admission_payload, parse_voice_context, verify_client_admission,
)


def signed_admission(*, issued=1000, expires=4600):
    value = {
        "version": 1, "room": "room-a", "participant": "client-a", "principal": "a" * 64,
        "issued_at": issued, "expires_at": expires,
        "nonce": "00000000-0000-4000-8000-000000000001",
        "voice": {"version": 1, "base": "Voice guidance.", "display": "Show supporting detail."},
    }
    value["signature"] = hmac.new(b"fixture-secret", admission_payload(value).encode(), hashlib.sha256).hexdigest()
    return value


def verify(value, **kwargs):
    return verify_client_admission(json.dumps({ADMISSION_KEY: value}), room="room-a",
        participant="client-a", key="fixture-secret", now=1001, **kwargs)


def test_signature_matches_the_api_cross_language_fixture():
    value = signed_admission()
    assert value["signature"] == "4565b4fe3fe1b8b8d3776ab570be272cceea66bf25eddbf60e9780b9482bc705"
    result = verify(value)
    assert result is not None
    assert result.room == "room-a" and result.participant == "client-a"
    assert result.voice.base == "Voice guidance."


@pytest.mark.parametrize("field,value", [
    ("room", "other"), ("participant", "other"), ("principal", "b" * 64),
    ("issued_at", 999), ("expires_at", 4601), ("version", True),
    ("nonce", "00000000-0000-4000-8000-000000000002"), ("signature", "0" * 64),
    ("voice", {"version": 1, "base": "Changed", "display": "Show supporting detail."}),
])
def test_tampering_never_admits_a_client(field, value):
    changed = copy.deepcopy(signed_admission())
    changed[field] = value
    assert verify(changed) is None


@pytest.mark.parametrize("issued,expires", [(0, 1001), (1032, 4600), (1000, 4601), (True, 4600), (1000, 1000)])
def test_even_a_signed_grant_must_have_a_bounded_current_lifetime(issued, expires):
    assert verify(signed_admission(issued=issued, expires=expires)) is None


@pytest.mark.parametrize("metadata", [None, "[]", "{}", "not JSON", "x" * 16385,
    '{"kortexa_voice_client":null,"kortexa_voice_client":{}}'])
def test_untrusted_metadata_fails_closed(metadata):
    assert verify_client_admission(metadata, room="room-a", participant="client-a",
        key="fixture-secret", now=1001) is None


def test_room_owner_and_key_are_required_even_with_an_intact_grant():
    metadata = json.dumps({ADMISSION_KEY: signed_admission()})
    for room, participant, key in [("room-b", "client-a", "fixture-secret"),
        ("room-a", "client-b", "fixture-secret"), ("room-a", "client-a", ""),
        ("room-a", "client-a", "wrong-secret")]:
        assert verify_client_admission(metadata, room=room, participant=participant, key=key, now=1001) is None


@pytest.mark.parametrize("value", [None, {}, {"version": True, "base": "x", "display": "y"},
    {"version": 1, "base": "x" * 2049, "display": "y"},
    {"version": 1, "base": "\ud800", "display": "y"}])
def test_voice_context_is_bounded_and_strict(value):
    assert parse_voice_context(value) is None
