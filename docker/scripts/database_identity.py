from __future__ import annotations

import json
import shlex
from dataclasses import asdict, dataclass
from typing import Mapping, Sequence
from urllib.parse import unquote, urlparse


class DatabaseIdentityError(RuntimeError):
    pass


@dataclass(frozen=True)
class DatabaseIdentity:
    host: str
    port: int
    database: str
    user: str
    connection_role: str

    def redacted(self) -> dict[str, str | int]:
        return asdict(self)


def _required(value: str | None, name: str) -> str:
    if value is None or not str(value).strip():
        raise DatabaseIdentityError(f"Database identity is missing {name}")
    return str(value).strip()


def _port(value: str | int | None, *, default: int = 5432) -> int:
    if value is None or str(value).strip() == "":
        return default
    try:
        port = int(value)
    except (TypeError, ValueError) as exc:
        raise DatabaseIdentityError("Database identity port must be an integer") from exc
    if not 1 <= port <= 65535:
        raise DatabaseIdentityError("Database identity port is outside 1..65535")
    return port


def _identity_from_url(url: str, connection_role: str) -> DatabaseIdentity:
    parsed = urlparse(url)
    if not parsed.scheme.startswith("postgresql"):
        raise DatabaseIdentityError(
            f"{connection_role} must use a PostgreSQL connection URL"
        )
    return DatabaseIdentity(
        host=_required(parsed.hostname, f"{connection_role} host"),
        port=_port(parsed.port),
        database=_required(parsed.path.lstrip("/"), f"{connection_role} database"),
        user=unquote(_required(parsed.username, f"{connection_role} user")),
        connection_role=connection_role,
    )


def parse_postgres_dsn(dsn: str, connection_role: str = "airflow_dsn") -> DatabaseIdentity:
    raw = _required(dsn, f"{connection_role} DSN")
    if "://" in raw:
        return _identity_from_url(raw, connection_role)

    values: dict[str, str] = {}
    try:
        tokens = shlex.split(raw)
    except ValueError as exc:
        raise DatabaseIdentityError(f"{connection_role} DSN is malformed") from exc
    for token in tokens:
        if "=" not in token:
            raise DatabaseIdentityError(f"{connection_role} DSN is malformed")
        key, value = token.split("=", 1)
        values[key.strip().lower()] = value.strip()
    return DatabaseIdentity(
        host=_required(values.get("host"), f"{connection_role} host"),
        port=_port(values.get("port")),
        database=_required(
            values.get("dbname") or values.get("database"),
            f"{connection_role} database",
        ),
        user=_required(values.get("user"), f"{connection_role} user"),
        connection_role=connection_role,
    )


def parse_jdbc_identity(
    jdbc_url: str,
    user: str,
    connection_role: str = "jupyter_jdbc",
) -> DatabaseIdentity:
    raw = _required(jdbc_url, f"{connection_role} URL")
    if not raw.startswith("jdbc:"):
        raise DatabaseIdentityError(f"{connection_role} URL must start with jdbc:")
    parsed = _identity_from_url(raw.removeprefix("jdbc:"), connection_role)
    return DatabaseIdentity(
        host=parsed.host,
        port=parsed.port,
        database=parsed.database,
        user=_required(user, f"{connection_role} user"),
        connection_role=connection_role,
    )


def parse_backend_identity(
    database_url: str,
    connection_role: str = "backend",
) -> DatabaseIdentity:
    return _identity_from_url(_required(database_url, "Backend DATABASE_URL"), connection_role)


def parse_endpoint_aliases(raw_aliases: str | None) -> frozenset[frozenset[str]]:
    aliases: set[frozenset[str]] = set()
    for raw_mapping in (raw_aliases or "").split(","):
        mapping = raw_mapping.strip()
        if not mapping:
            continue
        if "=" not in mapping:
            raise DatabaseIdentityError(
                "Endpoint aliases must use host:port=host:port mappings"
            )
        left, right = (side.strip().lower() for side in mapping.split("=", 1))
        if not left or not right or ":" not in left or ":" not in right:
            raise DatabaseIdentityError(
                "Endpoint aliases must use host:port=host:port mappings"
            )
        aliases.add(frozenset((left, right)))
    return frozenset(aliases)


def _endpoint(identity: DatabaseIdentity) -> str:
    return f"{identity.host.lower()}:{identity.port}"


def compare_database_identities(
    identities: Sequence[DatabaseIdentity],
    *,
    endpoint_aliases: frozenset[frozenset[str]] = frozenset(),
) -> dict[str, object]:
    if len(identities) < 2:
        raise DatabaseIdentityError("At least two database identities are required")
    reference = identities[0]
    errors: list[str] = []
    for candidate in identities[1:]:
        if candidate.database != reference.database:
            errors.append(
                f"database mismatch: {reference.connection_role}={reference.database} "
                f"{candidate.connection_role}={candidate.database}"
            )
        if candidate.user != reference.user:
            errors.append(
                f"user mismatch: {reference.connection_role}={reference.user} "
                f"{candidate.connection_role}={candidate.user}"
            )
        endpoints = frozenset((_endpoint(reference), _endpoint(candidate)))
        if len(endpoints) > 1 and endpoints not in endpoint_aliases:
            errors.append(
                f"endpoint mismatch: {reference.connection_role}={_endpoint(reference)} "
                f"{candidate.connection_role}={_endpoint(candidate)}"
            )
    report = {
        "valid": not errors,
        "identities": [identity.redacted() for identity in identities],
        "errors": errors,
    }
    if errors:
        raise DatabaseIdentityError(json.dumps(report, sort_keys=True))
    return report


def identities_from_environment(
    environment: Mapping[str, str],
) -> tuple[list[DatabaseIdentity], frozenset[frozenset[str]]]:
    identities = [
        parse_postgres_dsn(
            _required(
                environment.get("MODEL_PUBLICATION_DATABASE_DSN"),
                "MODEL_PUBLICATION_DATABASE_DSN",
            )
        ),
        parse_jdbc_identity(
            _required(
                environment.get("MODEL_PUBLICATION_JDBC_URL"),
                "MODEL_PUBLICATION_JDBC_URL",
            ),
            _required(
                environment.get("MODEL_PUBLICATION_DB_USER"),
                "MODEL_PUBLICATION_DB_USER",
            ),
        ),
        parse_backend_identity(
            _required(
                environment.get("MODEL_PUBLICATION_BACKEND_DATABASE_URL"),
                "MODEL_PUBLICATION_BACKEND_DATABASE_URL",
            )
        ),
    ]
    aliases = parse_endpoint_aliases(
        environment.get("MODEL_PUBLICATION_DB_ENDPOINT_ALIASES")
    )
    return identities, aliases


def verify_environment_identities(environment: Mapping[str, str]) -> dict[str, object]:
    identities, aliases = identities_from_environment(environment)
    return compare_database_identities(identities, endpoint_aliases=aliases)
