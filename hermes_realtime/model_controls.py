"""Voice model controls use Hermes's session model switch pipeline.

This small compatibility boundary is kept in the plugin: it does not create an
agent, parse a slash-command reply or write global profile configuration.
"""
from __future__ import annotations

import asyncio
import json
import re
from urllib.parse import urlsplit

import aiohttp

MODEL_ID = re.compile(r"[A-Za-z0-9._:/-]{1,128}")
EFFORTS = {"low", "medium", "high", "xhigh"}
PROVIDER = "kortexa.ai"


class ModelControlError(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def validate_selection(model, effort):
    if not isinstance(model, str) or not MODEL_ID.fullmatch(model):
        raise ModelControlError("invalid_model", "Choose a valid model.")
    if effort is not None and (not isinstance(effort, str) or effort not in EFFORTS):
        raise ModelControlError("unsupported_reasoning_effort", "Choose a supported reasoning effort.")


async def _catalog(provider):
    base, key = provider.get("api"), provider.get("api_key")
    if not isinstance(base, str) or not isinstance(key, str) or not key.strip():
        raise ModelControlError("model_provider_unavailable", "Configure the kortexa.ai provider in this Hermes profile.")
    url = urlsplit(base)
    if url.scheme not in {"http", "https"} or not url.hostname or url.username or url.password or url.query or url.fragment:
        raise ModelControlError("model_provider_unavailable", "The configured model provider URL is invalid.")
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as client:
            async with client.get(base.rstrip("/") + "/agent/models", headers={"Authorization": "Bearer " + key},
                                  allow_redirects=False) as response:
                if response.status != 200:
                    raise ValueError("catalog unavailable")
                raw = bytearray()
                async for chunk in response.content.iter_chunked(16384):
                    raw.extend(chunk)
                    if len(raw) > 256 * 1024:
                        raise ValueError("catalog too large")
                result = json.loads(raw)
                if not isinstance(result, dict) or not isinstance(result.get("data"), list):
                    raise ValueError("invalid catalog")
                return result["data"]
    except (aiohttp.ClientError, TimeoutError, ValueError):
        raise ModelControlError("model_catalog_unavailable", "The model catalog is unavailable. Try again.") from None


def resolve_selection(catalog, model, effort):
    validate_selection(model, effort)
    entry = next((item for item in catalog if isinstance(item, dict) and item.get("id") == model), None)
    if entry is None or entry.get("available") is not True:
        raise ModelControlError("model_unavailable", "The selected model is unavailable. Keep the current model or try later.")
    target = entry.get("alias_for", model)
    validate_selection(target, effort)
    capabilities = entry.get("capabilities") or {}
    if not isinstance(capabilities, dict):
        raise ModelControlError("model_catalog_unavailable", "The model catalog is invalid. Try again.")
    if effort is not None and capabilities.get("reasoning_effort") is not True:
        raise ModelControlError("unsupported_reasoning_effort", "This model does not support reasoning effort.")
    return target


async def select_model(adapter, source, model, effort, *, is_current=lambda: True):
    validate_selection(model, effort)
    runner = getattr(adapter, "gateway_runner", None)
    if runner is None or not all(callable(getattr(runner, name, None)) for name in (
        "_async_profile_scope_for_source", "_model_switch_lock", "_perform_model_switch", "_record_model_switch")):
        raise ModelControlError("model_controls_unavailable", "This Hermes version does not support voice model controls.")
    from gateway.slash_commands_model import _ModelSwitchContext
    from hermes_constants import get_hermes_home
    from hermes_cli.model_selection_guards import combined_selection_warning, selection_context_for_agent

    async with runner._async_profile_scope_for_source(source):
        async with runner._model_switch_lock():
            route = runner._session_key_for_source(source)
            ctx = _ModelSwitchContext(session_key=route, source=source,
                config_path=get_hermes_home() / "config.yaml", persist_global=False)
            ctx.read_config()
            provider = (ctx.user_provs or {}).get(PROVIDER) if isinstance(ctx.user_provs, dict) else None
            if not isinstance(provider, dict):
                raise ModelControlError("model_provider_unavailable", "Configure the kortexa.ai provider in this Hermes profile.")
            target = resolve_selection(await _catalog(provider), model, effort)
            ctx.apply_override(runner._session_model_override(route) or {})
            result, error = await runner._perform_model_switch(ctx, target, PROVIDER, source)
            if error is not None or result is None or not result.success or result.new_model != target:
                raise ModelControlError("model_resolution_failed", "Hermes could not select this model. The current selection is unchanged.")
            if (result.base_url or "").rstrip("/") != provider["api"].rstrip("/"):
                raise ModelControlError("model_resolution_failed", "Hermes resolved a different model provider.")
            warning = await asyncio.to_thread(combined_selection_warning, target, provider=result.target_provider,
                base_url=result.base_url, api_key=result.api_key, model_info=result.model_info,
                selection_context=selection_context_for_agent(runner._cached_agent_for(route)))
            if warning is not None:
                raise ModelControlError("model_confirmation_required", "This model change requires confirmation in Hermes.")
            if not is_current():
                raise ModelControlError("session_closed", "The voice session was replaced.")
            # This is the same next-turn commit used by Hermes's structured chat
            # picker. It evicts the cache without changing an in-flight agent.
            await runner._record_model_switch(result, ctx, source=source, one_turn=False, picker=True)
            from gateway.run import _platform_config_key
            runner._apply_reasoning_selection(route, _platform_config_key(source.platform), effort or "reset", persist_global=False)
            return {"model": target, "reasoning_effort": effort}
