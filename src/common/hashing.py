import hashlib
import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _json_default(obj: Any) -> Any:
    # Tipos que aparecem com frequência nos payloads/normalização.
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, datetime):
        # ISO-8601 com offset quando aplicável (datetime geralmente será UTC).
        return obj.isoformat()
    if is_dataclass(obj):
        return asdict(obj)
    # Deixa o json explodir se for realmente inesperado.
    raise TypeError(f"Type not serializable: {type(obj)}")


def stable_json_dumps(obj: Any) -> str:
    """
    Serialização determinística para hash.

    - `sort_keys=True` garante ordem consistente em dicts.
    - `separators=(',', ':')` reduz variações de formatação.
    """

    return json.dumps(obj, default=_json_default, sort_keys=True, separators=(",", ":"))


def payload_hash_from_event(event_dict: dict) -> str:
    """
    Calcula `sha256` a partir do JSON normalizado (determinístico).

    Importante: chame com o event_dict *sem* o campo `payload_hash`.
    """

    return sha256_hex(stable_json_dumps(event_dict))

