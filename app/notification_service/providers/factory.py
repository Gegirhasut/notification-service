"""Provider factory: select a mock provider by channel.

The factory builds one provider instance per channel and shares the same receipt
emitter + mode. Reusing instances preserves each provider's idempotency / attempt
state across messages within a worker process.
"""

from __future__ import annotations

from notification_service.config import ProviderMode
from notification_service.providers.base import BaseMockProvider, ReceiptEmitter
from notification_service.providers.email_mock import EmailMockProvider
from notification_service.providers.sms_mock import SmsMockProvider

_PROVIDERS: dict[str, type[BaseMockProvider]] = {
    "sms": SmsMockProvider,
    "email": EmailMockProvider,
}


class ProviderFactory:
    def __init__(
        self,
        *,
        mode: ProviderMode,
        emit_receipt: ReceiptEmitter,
        receipt_delay: float = 0.0,
        transient_attempts: int | None = None,
    ) -> None:
        self._instances: dict[str, BaseMockProvider] = {
            channel: cls(
                mode=mode,
                emit_receipt=emit_receipt,
                receipt_delay=receipt_delay,
                transient_attempts=transient_attempts,
            )
            for channel, cls in _PROVIDERS.items()
        }

    def for_channel(self, channel: str) -> BaseMockProvider:
        try:
            return self._instances[channel]
        except KeyError:
            raise ValueError(f"No provider for channel {channel!r}") from None

    def providers(self) -> list[BaseMockProvider]:
        """All provider instances (used to drain in-flight receipts on shutdown)."""
        return list(self._instances.values())
