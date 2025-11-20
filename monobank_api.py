# monobank_api.py

from typing import Any, Dict, List, Set, Tuple
import time
import requests
from datetime import datetime


MONOBANK_CLIENT_INFO_URL = "https://api.monobank.ua/personal/client-info"
MONOBANK_STATEMENT_URL = "https://api.monobank.ua/personal/statement/{account}/{from_ts}/{to_ts}"


def fetch_client_info(token: str) -> Dict[str, Any]:
    headers = {"X-Token": token}
    r = requests.get(MONOBANK_CLIENT_INFO_URL, headers=headers, timeout=10)
    r.raise_for_status()
    return r.json()


def fetch_statement(token: str, account_id: str, from_ts: int, to_ts: int) -> List[Dict[str, Any]]:
    """
    Забираем выписку, учитывая лимит Monobank (500 записей за запрос).
    Если вернулось 500, двигаем окно назад.
    """
    headers = {"X-Token": token}
    all_items: List[Dict[str, Any]] = []
    current_to = to_ts

    while True:
        url = MONOBANK_STATEMENT_URL.format(
            account=account_id,
            from_ts=from_ts,
            to_ts=current_to,
        )
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()

        batch = r.json()
        if not isinstance(batch, list):
            break
        if not batch:
            break

        all_items.extend(batch)

        if len(batch) < 500:
            break

        last_time = min(int(item.get("time", from_ts)) for item in batch)
        if last_time <= from_ts:
            break

        current_to = last_time - 1
        time.sleep(1)

    return all_items


def filter_income_and_ignore(
    items: List[Dict[str, Any]],
    ignore_ibans_norm: set[str],
    allow_in: bool = True,
    allow_out: bool = False,
) -> Tuple[List[Dict[str, Any]], Set[str]]:
    """
    Фильтрация операций по разрешённым направлениям и списку IBAN-исключений.
    Возвращает отфильтрованный список и множество фактически включённых типов потоков
    (subset of {"in", "out"}).
    """
    result: List[Dict[str, Any]] = []
    flows: Set[str] = set()

    for it in items:
        try:
            amount = int(it.get("amount", 0))
        except Exception:
            continue

        if amount == 0:
            continue

        counter_iban = (it.get("counterIban") or "").lower()
        if counter_iban and counter_iban in ignore_ibans_norm:
            continue

        if amount > 0:
            if not allow_in:
                continue
            flows.add("in")
            result.append(it)
        else:
            if not allow_out:
                continue
            flows.add("out")
            result.append(it)

    return result, flows


def unix_from_str(value: str, is_to: bool = False) -> int:
    """
    Преобразует строку в Unix time.
    - Если все цифры — считаем, что уже Unix time.
    - Иначе ожидаем ISO: YYYY-MM-DD или YYYY-MM-DDTHH:MM:SS.
      Если только дата и is_to=True, ставим 23:59:59.
    """
    value = value.strip()
    if value.isdigit():
        return int(value)

    dt = datetime.fromisoformat(value)
    if is_to and dt.hour == 0 and dt.minute == 0 and dt.second == 0:
        dt = dt.replace(hour=23, minute=59, second=59)

    return int(dt.timestamp())
