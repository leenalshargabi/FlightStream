# FlightStream OpenSky API client

# Fetches live aircraft state vectors from OpenSky and converts the raw
# state-vector arrays into JSON-friendly dictionaries for the Kafka producer

# Secrets are loaded from the project .env file and are never written to output


from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

import requests
from dotenv import load_dotenv

# Load variables from C:\Big data\FlightStream\.env when run from the project root
load_dotenv()

TOKEN_URL = (
    "https://auth.opensky-network.org/"
    "auth/realms/opensky-network/protocol/openid-connect/token"
)
STATES_URL = "https://opensky-network.org/api/states/all"

# Global /states/all requests cost credits. Two minutes is fast enough for a
# streaming demo while keeping the daily request volume reasonable
DEFAULT_POLL_INTERVAL_SECONDS = 120
REQUEST_TIMEOUT_SECONDS = 30
TOKEN_REFRESH_MARGIN_SECONDS = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("flightstream.api")


class OpenSkyError(RuntimeError):
    """Raised when the OpenSky API cannot provide flight data."""


class TokenManager:
    """Manage OpenSky OAuth2 access tokens and refresh them automatically."""

    def __init__(self, client_id: str, client_secret: str) -> None:
        if not client_id or not client_secret:
            raise ValueError(
                "OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET must be set in .env"
            )

        self.client_id = client_id
        self.client_secret = client_secret
        self._token: str | None = None
        self._expires_at: datetime | None = None

    def get_token(self) -> str:
        """Return a valid token, refreshing it when necessary."""
        now = datetime.now(timezone.utc)

        if (
            self._token
            and self._expires_at
            and now < self._expires_at
        ):
            return self._token

        return self._refresh()

    def _refresh(self) -> str:
        logger.info("Requesting a new OpenSky access token...")

        response = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise OpenSkyError(
                f"OpenSky authentication failed: HTTP {response.status_code}"
            ) from exc

        data = response.json()
        token = data.get("access_token")
        expires_in = int(data.get("expires_in", 1800))

        if not token:
            raise OpenSkyError("OpenSky authentication response did not contain a token")

        self._token = token
        self._expires_at = datetime.now(timezone.utc) + timedelta(
            seconds=max(1, expires_in - TOKEN_REFRESH_MARGIN_SECONDS)
        )

        logger.info("OpenSky authentication successful.")
        return token

    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.get_token()}"}


class OpenSkyClient:
    """Small client responsible only for retrieving and normalizing OpenSky data."""

    def __init__(self, token_manager: TokenManager) -> None:
        self.token_manager = token_manager
        self.session = requests.Session()

    def get_states(self) -> dict[str, Any]:
        """Retrieve the current global aircraft state snapshot."""
        response = self.session.get(
            STATES_URL,
            headers=self.token_manager.headers(),
            timeout=REQUEST_TIMEOUT_SECONDS,
        )

        # OpenSky documents 401 as an expired/invalid token. Refresh once and retry
        if response.status_code == 401:
            logger.info("OpenSky token expired; refreshing and retrying once...")
            self.token_manager._refresh()
            response = self.session.get(
                STATES_URL,
                headers=self.token_manager.headers(),
                timeout=REQUEST_TIMEOUT_SECONDS,
            )

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            if response.status_code == 429:
                retry_after = response.headers.get(
                    "X-Rate-Limit-Retry-After-Seconds", "unknown"
                )
                raise OpenSkyError(
                    f"OpenSky rate limit reached. Retry after {retry_after} seconds."
                ) from exc
            raise OpenSkyError(
                f"OpenSky flight request failed: HTTP {response.status_code}"
            ) from exc

        data = response.json()
        if not isinstance(data, dict):
            raise OpenSkyError("OpenSky returned an unexpected response format")

        return data

    def get_flights(self) -> list[dict[str, Any]]:
        """Retrieve and normalize the current aircraft snapshot."""
        payload = self.get_states()
        snapshot_time = payload.get("time")
        states = payload.get("states") or []

        flights: list[dict[str, Any]] = []
        for state in states:
            record = state_to_record(state, snapshot_time)
            if record is not None:
                flights.append(record)

        logger.info(
            "Received %d aircraft records from OpenSky.", len(flights)
        )
        return flights

    def close(self) -> None:
        self.session.close()


def _clean_callsign(value: Any) -> str | None:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def state_to_record(
    state: list[Any] | tuple[Any, ...], snapshot_time: Any
) -> dict[str, Any] | None:
    """Convert an OpenSky state-vector array into a named record.

    OpenSky's standard state vector currently contains indexes 0-16 in the
    response used by this project. The optional aircraft category can appear
    at index 17 when the extended query is enabled.
    """
    if not state or len(state) < 17:
        return None

    icao24 = state[0]
    if icao24 is None or not str(icao24).strip():
        return None

    return {
        "icao24": str(icao24).strip().lower(),
        "callsign": _clean_callsign(state[1]),
        "origin_country": state[2],
        "time_position": state[3],
        "last_contact": state[4],
        "longitude": state[5],
        "latitude": state[6],
        "baro_altitude": state[7],
        "on_ground": state[8],
        "velocity": state[9],
        "true_track": state[10],
        "vertical_rate": state[11],
        "sensors": state[12],
        "geo_altitude": state[13],
        "squawk": state[14],
        "spi": state[15],
        "position_source": state[16],
        "category": state[17] if len(state) > 17 else None,
        "snapshot_time": snapshot_time,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def create_client() -> OpenSkyClient:
    """Create an OpenSky client using credentials from the environment."""
    client_id = os.getenv("OPENSKY_CLIENT_ID")
    client_secret = os.getenv("OPENSKY_CLIENT_SECRET")

    token_manager = TokenManager(client_id or "", client_secret or "")
    return OpenSkyClient(token_manager)


def stream_flights(
    poll_interval: int = DEFAULT_POLL_INTERVAL_SECONDS,
) -> Iterator[list[dict[str, Any]]]:
    """Yield a new live flight snapshot continuously.

    The Kafka producer will consume this generator later. The API layer does
    not know anything about Kafka.
    """
    if poll_interval < 10:
        raise ValueError("poll_interval must be at least 10 seconds")

    client = create_client()
    try:
        while True:
            started = time.monotonic()

            try:
                yield client.get_flights()
            except (requests.RequestException, OpenSkyError) as exc:
                logger.error("OpenSky request failed: %s", exc)

            elapsed = time.monotonic() - started
            sleep_for = max(0, poll_interval - elapsed)
            logger.info("Next OpenSky snapshot in %.0f seconds.", sleep_for)
            time.sleep(sleep_for)
    finally:
        client.close()


def main() -> None:
    """Run a simple local API smoke test without starting Kafka."""
    client = create_client()
    try:
        flights = client.get_flights()
        print(f"OpenSky snapshot received: {len(flights)} aircraft")

        if flights:
            print("First normalized record:")
            print(flights[0])
    finally:
        client.close()


if __name__ == "__main__":
    main()
