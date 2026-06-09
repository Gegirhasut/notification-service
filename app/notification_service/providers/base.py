"""Provider protocol and shared result/record types.

A provider simulates an external SMS/Email gateway. `send()` models synchronous
gateway *acceptance* (returning a provider_message_id), then asynchronously emits
a delivery *receipt* (delivered/rejected) onto the receipts queue. The dispatcher
treats acceptance and final delivery as two distinct stages, exactly as a real
gateway with webhooks would.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from notification_service.config import ProviderMode

logger = logging.getLogger(__name__)

# Async callable the provider uses to emit a delivery receipt back onto the
# receipts queue: (notification_id, outcome, provider_message_id, detail).
ReceiptEmitter = Callable[[str, str, str | None, str | None], Awaitable[None]]


@dataclass(slots=True)
class SendResult:
    """Outcome of the synchronous gateway acceptance call."""

    accepted: bool
    provider_message_id: str | None = None
    # transient=True signals a retryable failure (e.g. gateway 503). When
    # accepted is False and transient is False, the failure is permanent.
    transient: bool = False
    error: str | None = None


@dataclass(slots=True)
class CallRecord:
    notification_id: str
    recipient: str
    body: str


@runtime_checkable
class Provider(Protocol):
    channel: str

    async def send(self, notification_id: str, recipient: str, body: str) -> SendResult: ...


@dataclass
class RecordingMixin:
    """Records every send() call so tests can assert provider + args."""

    calls: list[CallRecord] = field(default_factory=list)

    def _record(self, notification_id: str, recipient: str, body: str) -> None:
        self.calls.append(CallRecord(notification_id, recipient, body))


class BaseMockProvider(RecordingMixin):
    """Shared mock-gateway behaviour for the SMS/Email providers.

    Deterministic per ``PROVIDER_MODE`` so tests can force an outcome. The mock
    is idempotent on ``notification_id`` (invariant 2): a repeated send for an
    already-accepted id returns the cached result and does not re-emit a receipt.
    """

    channel: str = "base"

    def __init__(
        self,
        *,
        mode: ProviderMode,
        emit_receipt: ReceiptEmitter,
        receipt_delay: float = 0.0,
        transient_attempts: int | None = None,
    ) -> None:
        self.calls = []
        self._mode = mode
        self._emit_receipt = emit_receipt
        self._receipt_delay = receipt_delay
        # Number of leading attempts that fail transiently before delivering.
        # None -> derive from mode (1 for transient_then_deliver, else 0). Tests
        # set a large value to force retry exhaustion deterministically.
        self._transient_attempts = transient_attempts
        self._attempts: dict[str, int] = {}
        self._results: dict[str, SendResult] = {}
        self._tasks: set[asyncio.Task] = set()
        # Effective sends per id: an accepted send that actually emitted a
        # receipt. Deduplication on the idempotency key (notification.id) keeps
        # this at exactly one even if send() is invoked repeatedly across retries
        # and duplicates. Tests assert on this to prove provider-key exactly-once.
        self.effective_sends: dict[str, int] = {}

    def _decide(self, notification_id: str, attempt: int) -> str:
        """Return one of 'deliver' | 'reject' | 'transient'."""
        if self._transient_attempts is not None:
            return "transient" if attempt <= self._transient_attempts else "deliver"

        match self._mode:
            case ProviderMode.always_deliver:
                return "deliver"
            case ProviderMode.always_reject:
                return "reject"
            case ProviderMode.transient_then_deliver:
                return "transient" if attempt == 1 else "deliver"
            case _:  # random
                roll = random.random()
                if roll < 0.90:
                    return "deliver"
                if roll < 0.95:
                    return "reject"
                return "transient"

    async def send(self, notification_id: str, recipient: str, body: str) -> SendResult:
        self._record(notification_id, recipient, body)

        # Provider-key idempotency: an already-accepted id returns its cached
        # result and does NOT emit another receipt. This is the exactly-once
        # backstop for the window where the row is back in 'queued' mid-retry and
        # the CAS gate alone can no longer block a concurrent duplicate. There is
        # deliberately no `await` between this check and the cache write below, so
        # two concurrent send() calls for one id cannot both reach acceptance.
        if notification_id in self._results:
            return self._results[notification_id]

        attempt = self._attempts.get(notification_id, 0) + 1
        self._attempts[notification_id] = attempt

        outcome = self._decide(notification_id, attempt)
        if outcome == "transient":
            return SendResult(
                accepted=False,
                transient=True,
                error="gateway temporarily unavailable",
            )

        provider_message_id = f"{self.channel}-mock-{notification_id}"
        result = SendResult(accepted=True, provider_message_id=provider_message_id)
        self._results[notification_id] = result
        self.effective_sends[notification_id] = self.effective_sends.get(notification_id, 0) + 1

        receipt_outcome = "delivered" if outcome == "deliver" else "rejected"
        detail = None if outcome == "deliver" else "rejected by gateway"
        self._spawn_receipt(notification_id, provider_message_id, receipt_outcome, detail)
        return result

    def _spawn_receipt(
        self, notification_id: str, provider_message_id: str, outcome: str, detail: str | None
    ) -> None:
        task = asyncio.create_task(
            self._emit_after_delay(notification_id, provider_message_id, outcome, detail)
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _emit_after_delay(
        self, notification_id: str, provider_message_id: str, outcome: str, detail: str | None
    ) -> None:
        try:
            if self._receipt_delay:
                await asyncio.sleep(self._receipt_delay)
            await self._emit_receipt(notification_id, outcome, provider_message_id, detail)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Failed to emit receipt for %s", notification_id)

    async def drain(self) -> None:
        """Await all in-flight receipt emissions (used by tests)."""
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
