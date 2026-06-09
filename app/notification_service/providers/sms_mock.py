"""Mock SMS gateway provider."""

from __future__ import annotations

from notification_service.providers.base import BaseMockProvider


class SmsMockProvider(BaseMockProvider):
    channel = "sms"
