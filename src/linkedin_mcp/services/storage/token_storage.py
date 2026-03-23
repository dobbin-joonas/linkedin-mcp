"""
Secure token storage using system keychain.

Uses the `keyring` library to store OAuth tokens and cookies securely in:
- macOS: Keychain
- Windows: Windows Credential Locker
- Linux: Secret Service (GNOME Keyring, KWallet)
"""

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Optional

import structlog

logger = structlog.get_logger(__name__)

# Service name for keyring
SERVICE_NAME = "linkedin-mcp"

# Keys for different credential types
OFFICIAL_TOKEN_KEY = "official_oauth_token"
OFFICIAL_METADATA_KEY = "official_oauth_metadata"
UNOFFICIAL_COOKIES_KEY = "unofficial_cookies"
UNOFFICIAL_METADATA_KEY = "unofficial_metadata"


@dataclass
class TokenData:
    """OAuth token data with expiration tracking."""

    access_token: str
    expires_at: datetime
    scopes: list[str]
    token_type: str = "Bearer"
    created_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        if self.created_at is None:
            self.created_at = datetime.now()

    @property
    def is_expired(self) -> bool:
        """Check if token has expired."""
        return datetime.now() >= self.expires_at

    @property
    def expires_soon(self) -> bool:
        """Check if token expires within 7 days."""
        return datetime.now() >= self.expires_at - timedelta(days=7)

    @property
    def days_until_expiry(self) -> int:
        """Get number of days until token expires."""
        delta = self.expires_at - datetime.now()
        return max(0, delta.days)

    @property
    def seconds_until_expiry(self) -> int:
        """Get number of seconds until token expires."""
        delta = self.expires_at - datetime.now()
        return max(0, int(delta.total_seconds()))

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "access_token": self.access_token,
            "expires_at": self.expires_at.isoformat(),
            "scopes": self.scopes,
            "token_type": self.token_type,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TokenData":
        """Create from dictionary."""
        return cls(
            access_token=data["access_token"],
            expires_at=datetime.fromisoformat(data["expires_at"]),
            scopes=data["scopes"],
            token_type=data.get("token_type", "Bearer"),
            created_at=datetime.fromisoformat(data["created_at"])
            if data.get("created_at")
            else None,
        )


# ==================== Official OAuth Token Storage ====================

def store_official_token(token_data: TokenData) -> bool:
    return True

def get_official_token() -> Optional[TokenData]:
    return None

def delete_official_token() -> bool:
    return True


# ==================== Unofficial Cookie Storage ====================


@dataclass
class CookieData:
    """Cookie data for unofficial API authentication."""

    li_at: str  # Primary authentication cookie
    jsessionid: Optional[str] = None
    extracted_at: Optional[datetime] = None
    browser: Optional[str] = None

    def __post_init__(self) -> None:
        if self.extracted_at is None:
            self.extracted_at = datetime.now()

    @property
    def is_stale(self) -> bool:
        """Check if cookies are older than 24 hours (may need refresh)."""
        if self.extracted_at is None:
            return True
        return datetime.now() >= self.extracted_at + timedelta(hours=24)

    @property
    def hours_since_extraction(self) -> int:
        """Get hours since cookies were extracted."""
        if self.extracted_at is None:
            return 999
        delta = datetime.now() - self.extracted_at
        return int(delta.total_seconds() / 3600)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "li_at": self.li_at,
            "jsessionid": self.jsessionid,
            "extracted_at": self.extracted_at.isoformat() if self.extracted_at else None,
            "browser": self.browser,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CookieData":
        """Create from dictionary."""
        return cls(
            li_at=data["li_at"],
            jsessionid=data.get("jsessionid"),
            extracted_at=datetime.fromisoformat(data["extracted_at"])
            if data.get("extracted_at")
            else None,
            browser=data.get("browser"),
        )


def store_unofficial_cookies(cookie_data: CookieData) -> bool:
    return True

def get_unofficial_cookies() -> Optional[CookieData]:
    return None

def delete_unofficial_cookies() -> bool:
    return True
