"""One-time OAuth consent flow.

Run this on a machine with a browser:

    poetry run python -m broker_scout.tools.oauth_setup

It opens your default browser, walks you through the Google consent
screen (sign in, click Allow), receives the auth code on a localhost
redirect, exchanges it for a refresh token, and writes the resulting
credentials to `secrets/oauth_token.json`.

After that, copy `secrets/oauth_token.json` (and the rest of the
secrets/ directory) to wherever the spider runs. The refresh token is
long-lived; the running process never needs a browser again.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from broker_scout.utils.gauth import DEFAULT_TOKEN_PATH, SCOPES
from broker_scout.utils.logging_setup import configure_logging

logger = logging.getLogger("oauth_setup")

DEFAULT_CLIENT_PATH = "secrets/oauth_client.json"


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the one-time OAuth consent flow.")
    parser.add_argument(
        "--client",
        default=None,
        help="Path to the OAuth client JSON downloaded from Google Cloud "
        f"Console (default: $OAUTH_CLIENT_JSON_PATH or {DEFAULT_CLIENT_PATH!r}).",
    )
    parser.add_argument(
        "--out",
        default=None,
        help="Where to write the resulting token JSON "
        f"(default: $OAUTH_TOKEN_JSON_PATH or {DEFAULT_TOKEN_PATH!r}).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=0,
        help="Local port for the OAuth redirect (default: 0 = random free port).",
    )
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    load_dotenv()
    args = _parse_args(argv)

    client_path = args.client or os.getenv("OAUTH_CLIENT_JSON_PATH", DEFAULT_CLIENT_PATH)
    token_path = args.out or os.getenv("OAUTH_TOKEN_JSON_PATH", DEFAULT_TOKEN_PATH)

    if not os.path.exists(client_path):
        logger.error(
            "OAuth client JSON not found at %r — download it from Google "
            "Cloud Console (APIs & Services → Credentials → OAuth client ID, "
            "Desktop app) and save it there. See README §'One-time Google "
            "integration setup' for full instructions.",
            client_path,
        )
        return 1

    # Imported lazily so `--help` works without the optional dep.
    from google_auth_oauthlib.flow import InstalledAppFlow

    flow = InstalledAppFlow.from_client_secrets_file(client_path, list(SCOPES))
    logger.info("opening browser for consent — sign in and click Allow")
    creds = flow.run_local_server(port=args.port, open_browser=True)

    out = Path(token_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(creds.to_json())
    # Restrict file permissions: refresh token is the credential.
    os.chmod(out, 0o600)

    logger.info(
        "wrote oauth token",
        extra={"path": str(out), "scopes": list(creds.scopes or [])},
    )
    print(
        f"\nOK. Token saved to {out}.\n"
        "Treat this file like a password — it grants access to the "
        "consenting Google account's Sheets + Drive.\n"
        "Copy it to your server alongside the rest of the repo when "
        "you deploy.\n"
    )
    return 0


def main() -> None:
    configure_logging("INFO")
    sys.exit(run())


if __name__ == "__main__":
    main()
