"""Stub adapter for Signal via signal-cli.

Group assignment: implement on_message and send against the mock-API
fake in tests/channels/mocks/signal_mock.py. See docs/ADAPTER_GUIDE.md
for the standard workflow.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from datetime import datetime
from typing import Any

from glc.channels.base import ChannelAdapter
from glc.channels.catalogue.signal.schemas import (
    SendParams,
    SignalReceiveNotification,
    SignalSendRequest,
)
from glc.channels.envelope import ChannelMessage, ChannelReply
from glc.security.allowlists import allowed
from glc.security.pairing import get_pairing_store
from glc.security.trust_level import classify
from glc.security.pairing import get_pairing_store
from glc.security.trust_level import classify
from glc.channels.catalogue.signal.schemas import SendParams, SignalSendRequest


class Adapter(ChannelAdapter):
    name = "signal"

    async def on_message(self, raw: Any) -> ChannelMessage | None:
        # signal-cli can drop the JSON-RPC socket; the mock signals this via
        # pop_disconnect(). Acknowledge the reconnect and keep processing the
        # event instead of raising.
        mock = self.config.get("mock")
        if mock is not None and hasattr(mock, "pop_disconnect"):
            mock.pop_disconnect()

        notification = SignalReceiveNotification.model_validate(raw)
        params = notification.params
        envelope = params.envelope if params else None
        if envelope is None or not envelope.source:
            return None

        channel_user_id = envelope.source
        data_message = envelope.data_message
        text = data_message.message if data_message else None

        group_id: str | None = None
        if data_message and data_message.group_info:
            group_id = data_message.group_info.group_id

        trust_level = classify(self.name, channel_user_id)

        # In public channels the default posture is owner-/allowlist-only.
        # Consult the allowlist before surfacing strangers; silently drop
        # senders who are not permitted.
        if self.config.get("is_public_channel"):
            owner_ids = [rec.channel_user_id for rec in get_pairing_store().owners(self.name)]
            ok, _reason = allowed(
                self.name,
                channel_user_id,
                owner_ids=owner_ids,
                is_public_channel=True,
            )
            if not ok:
                return None

        arrived_at = self._arrived_at(envelope, data_message)

        metadata: dict[str, Any] = {}
        if group_id:
            metadata["signal_group_id"] = group_id

        return ChannelMessage(
            channel=self.name,
            channel_user_id=channel_user_id,
            user_handle=envelope.source_name or channel_user_id,
            text=text,
            thread_id=group_id,
            trust_level=trust_level,
            arrived_at=arrived_at,
            metadata=metadata,
        )

    @staticmethod
    def _arrived_at(envelope: Any, data_message: Any) -> datetime:
        # signal-cli timestamps are epoch milliseconds.
        ts_ms = None
        if data_message is not None and data_message.timestamp is not None:
            ts_ms = data_message.timestamp
        elif envelope is not None and envelope.timestamp is not None:
            ts_ms = envelope.timestamp
        if ts_ms is None:
            return datetime.now(timezone.utc)
        return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

    async def send(self, reply: ChannelReply) -> Any:
        params = SendParams(
            message=reply.text or "",
            recipient=reply.channel_user_id if not reply.thread_id else None,
            group_id=reply.thread_id if reply.thread_id else None,
        )
        request = SignalSendRequest(id=uuid.uuid4().hex, params=params)
            group_id=reply.thread_id if reply.thread_id else None
        )

        request = SignalSendRequest(
            id=uuid.uuid4().hex,
            params=params
        )

        payload = request.model_dump(by_alias=True, exclude_none=True)

        mock = self.config.get("mock")
        if mock is not None:
            return await mock.send(payload)

        return payload
