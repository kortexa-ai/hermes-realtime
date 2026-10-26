import base64
import json
import pytest

from hermes_realtime.client_image import decode_client_image

DATA = bytes([0xff, 0xd8, 0xff, 0xc0, 0, 8, 8, 0, 4, 0, 6, 1, 0xff, 0xd9])
IMAGE = {"type": "client_image", "version": 1, "mime_type": "image/jpeg", "data": base64.b64encode(DATA).decode(), "width": 6, "height": 4, "text": "Synthetic snapshot"}


def test_client_image_is_a_native_multimodal_result_and_plain_text_is_unchanged():
    decoded = decode_client_image(json.dumps(IMAGE))
    assert decoded["_multimodal"] is True
    assert decoded["text_summary"] == "Synthetic snapshot"
    assert decoded["content"][1]["image_url"]["url"] == "data:image/jpeg;base64," + IMAGE["data"]
    assert decode_client_image("ordinary tool text") == "ordinary tool text"


@pytest.mark.parametrize("change", [{"width": 5}, {"width": 641}, {"version": True}, {"data": "x" * 13337}, {"url": "https://private.invalid"}, {"mime_type": "image/png"}])
def test_client_image_rejects_wrong_dimensions_budgets_and_remote_sources(change):
    with pytest.raises(ValueError, match="Invalid compact"):
        decode_client_image({**IMAGE, **change})
