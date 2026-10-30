"""Who is calling, and with which roles (viewer, data_owner, engineer).

Identity comes from, in order:
  1. Identity-Aware Proxy header (X-Goog-Authenticated-User-Email) when IAP fronts the service
  2. the Cloud Run IAM-verified bearer token (Cloud Run has already checked the signature;
     we only read the email claim)
  3. LOCAL_USER when ENV=local
"""
from __future__ import annotations

import base64
import json
from dataclasses import dataclass

from fastapi import Depends, HTTPException, Request

from .config import Settings, get_settings

ROLES = ("viewer", "data_owner", "engineer")


@dataclass(frozen=True)
class User:
    email: str
    roles: frozenset[str]

    def has(self, role: str) -> bool:
        return role in self.roles


def _parse_role_map(raw: str) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for part in filter(None, (p.strip() for p in raw.split(";"))):
        email, _, roles = part.partition("=")
        out[email.strip().lower()] = {r.strip() for r in roles.split(",") if r.strip() in ROLES}
    return out


def _email_from_bearer(header: str) -> str | None:
    if not header.lower().startswith("bearer "):
        return None
    parts = header.split()[1].split(".")
    if len(parts) < 2:
        return None
    try:
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))
    except ValueError:
        return None
    return payload.get("email")


def current_user(request: Request, settings: Settings = Depends(get_settings)) -> User:
    email = None
    iap = request.headers.get("x-goog-authenticated-user-email")
    if iap:
        email = iap.split(":", 1)[-1]
    if not email:
        email = _email_from_bearer(request.headers.get("authorization", "")) or \
            _email_from_bearer(request.headers.get("x-serverless-authorization", ""))
    if not email and settings.env == "local":
        return User(settings.local_user, frozenset(ROLES))
    if not email:
        raise HTTPException(401, "No authenticated identity")
    roles = _parse_role_map(settings.role_map).get(email.lower())
    if roles is None:
        roles = {r.strip() for r in settings.default_roles.split(",") if r.strip()}
    return User(email, frozenset(roles | {"viewer"}))


def require(role: str):
    def dep(user: User = Depends(current_user)) -> User:
        if not user.has(role):
            raise HTTPException(403, f"Requires the {role} role")
        return user
    return dep
