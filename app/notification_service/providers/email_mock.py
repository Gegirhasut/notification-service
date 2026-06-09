"""Mock Email gateway provider."""

from __future__ import annotations

from notification_service.providers.base import BaseMockProvider


class EmailMockProvider(BaseMockProvider):
    channel = "email"
