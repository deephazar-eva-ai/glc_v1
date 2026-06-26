"""Signal adapter — signal-cli JSON-RPC wire format.

Inbound:  JSON-RPC notification, method "receive".
          params.envelope.source       → sender phone (E.164)
          params.envelope.dataMessage.message → text body
          params.envelope.dataMessage.groupInfo.groupId → group (base64)

Outbound: JSON-RPC request, method "send".
          DM:    params = {recipient, message}
          Group: params = {groupId, message}

Required env vars: SIGNAL_CLI_PATH, SIGNAL_ACCOUNT_NUMBER
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from glc.channels.base import ChannelAdapter
from glc.channels.envelope import ChannelMessage, ChannelReply
from glc.security.trust_level import classify
from glc.channels.catalogue.signal.schemas import (
    SendParams,
    SignalReceiveNotification,
    SignalSendRequest,
)


class Adapter(ChannelAdapter):
    name = "signal"

    async def on_message(self, raw: Any) -> ChannelMessage:
        mock = self.config.get("mock")

        # Safe disconnect handling — not all client objects expose pop_disconnect.
        pop = getattr(mock, "pop_disconnect", None)
        if pop and pop():
            pass  # real impl would close + reopen the socket here

        # Skip notifications that are not inbound data messages
        # (typing indicators, read receipts, sync events, etc.)
        if raw.get("method") != "receive":
            return None  # type: ignore[return-value]

        notification = SignalReceiveNotification.model_validate(raw)
        params = notification.params
        envelope = params.envelope if params else None

        if envelope is None:
            return None  # type: ignore[return-value]

        source = envelope.source or ""
        if not source:
            return None  # type: ignore[return-value]

        source_name = envelope.source_name or ""
        timestamp_ms = envelope.timestamp or 0
        data_message = envelope.data_message

        # Envelopes without dataMessage carry no text (receipts, call events).
        if data_message is None:
            return None  # type: ignore[return-value]

        # Normalise empty string to None so callers can use `if msg.text`.
        text = data_message.message or None
        group_id = data_message.group_info.group_id if data_message.group_info else None

        # Use UTC now as fallback when timestamp is absent or zero.
        arrived_at = (
            datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
            if timestamp_ms
            else datetime.now(tz=timezone.utc)
        )

        trust = classify("signal", source)

        metadata: dict[str, Any] = {}
        if group_id:
            metadata["signal_group_id"] = group_id

        return ChannelMessage(
            channel="signal",
            channel_user_id=source,
            user_handle=source_name,
            text=text,
            trust_level=trust,
            arrived_at=arrived_at,
            metadata=metadata,
        )

    async def send(self, reply: ChannelReply) -> Any:
        # Guard: nothing to send
        if not reply.text and not reply.attachments:
            raise ValueError("send: reply must have text or at least one attachment")

        # Guard: DM with no destination
        if not reply.thread_id and not reply.channel_user_id:
            raise ValueError("send: DM reply requires a non-empty channel_user_id")

        # Build params dict using wire-format alias keys ("groupId", not "group_id"),
        # because SendParams uses alias= without populate_by_name=True — passing
        # by Python name would be treated as extra and silently dropped.
        params_dict: dict[str, Any] = {"message": reply.text or ""}
        if reply.thread_id:
            params_dict["groupId"] = reply.thread_id
        else:
            params_dict["recipient"] = reply.channel_user_id
        params = SendParams.model_validate(params_dict)

        request = SignalSendRequest(
            id=uuid.uuid4().hex,
            params=params,
        )

        payload = request.model_dump(by_alias=True, exclude_none=True)

        mock = self.config.get("mock")
        if mock is not None:
            return await mock.send(payload)

        return payload
