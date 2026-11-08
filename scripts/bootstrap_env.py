"""Create ``.env`` from ``.env.example`` with strong random secrets, and render the
SeaweedFS identity file from its template. Idempotent: existing values are never replaced.

Usage: python scripts/bootstrap_env.py
"""

from __future__ import annotations

import base64
import re
import secrets
import string
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / ".env.example"
ENV = ROOT / ".env"
S3_TEMPLATE = ROOT / "infra" / "seaweedfs" / "s3.json.tmpl"
S3_RENDERED = ROOT / "infra" / "seaweedfs" / "s3.json"

_ALNUM = string.ascii_letters + string.digits


def _random_value(key: str) -> str:
    if key == "FINTXN_TOKEN_KEY":
        return secrets.token_hex(32)
    if key == "FINTXN_ENCRYPTION_KEY":
        return base64.b64encode(secrets.token_bytes(32)).decode()
    if key.endswith("ACCESS_KEY"):
        return "".join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(20))
    return "".join(secrets.choice(_ALNUM) for _ in range(40))


def _parse(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def bootstrap_env() -> dict[str, str]:
    existing = _parse(ENV)
    lines: list[str] = []
    for raw_line in EXAMPLE.read_text().splitlines():
        out_line = raw_line
        if "=" in raw_line and not raw_line.lstrip().startswith("#"):
            k, v = raw_line.split("=", 1)
            k = k.strip()
            if existing.get(k):
                v = existing[k]
            elif v.startswith("change-me") or (v == "" and k.startswith(("FINTXN_", "S3_"))):
                v = _random_value(k)
            out_line = f"{k}={v}"
        lines.append(out_line)
    ENV.write_text("\n".join(lines) + "\n")
    ENV.chmod(0o600)
    return _parse(ENV)


def render_s3_config(values: dict[str, str]) -> None:
    text = S3_TEMPLATE.read_text()
    rendered = re.sub(r"\$\{([A-Z0-9_]+)\}", lambda m: values.get(m.group(1), ""), text)
    S3_RENDERED.write_text(rendered)
    S3_RENDERED.chmod(0o600)


if __name__ == "__main__":
    vals = bootstrap_env()
    render_s3_config(vals)
    print(f"wrote {ENV.relative_to(ROOT)} and {S3_RENDERED.relative_to(ROOT)} (both git-ignored)")
