# bot.py

import os
import time
import logging
import calendar
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta, date
from calendar import monthrange

from requests import HTTPError
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
    CallbackQuery,
    Message,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

from config import TELEGRAM_BOT_TOKEN
from db import (
    upsert_user_on_start,
    get_user,
    update_user_role,
    get_accounts_for_user,
    get_account_by_id,
    get_organization_by_id,
    get_ignore_ibans_norm,
    list_admin_ids,
    is_admin,
    insert_organization,
    list_organizations,
    insert_account,
    list_accounts_by_org,
    list_all_active_accounts,
    list_users,
    grant_account_to_user,
    revoke_account_from_user,
    get_user_account_permissions_map,
    update_user_account_permissions,
    update_user_friendly_name,
    log_user_action,
)
from i18n import DEFAULT_LANGUAGE, Translator, get_translator_for_user
from monobank_api import (
    unix_from_str,
    fetch_statement,
    filter_income_and_ignore,
    fetch_client_info,
)
from report_xlsx import write_xlsx

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


def _translator_from_update(update: Update) -> tuple[Translator, Dict[str, Any] | None]:
    user_row: Dict[str, Any] | None = None
    if update and update.effective_user:
        user_row = get_user(update.effective_user.id)
    translator = get_translator_for_user(user_row)
    return translator, user_row

STATEMENT_MIN_INTERVAL = 60  # секунды – лимит Monobank на выписку по одному токену

def get_custom_period_help(translator: Translator) -> str:
    return translator.t("period.custom_help")


# --- Вспомогательные функции / меню ---


def build_main_menu(role: str, translator: Translator | None = None) -> ReplyKeyboardMarkup:
    translator = translator or Translator(DEFAULT_LANGUAGE)
    buttons = [
        [
            KeyboardButton(translator.t("main.payments")),
            KeyboardButton(translator.t("main.statement")),
        ],
        [KeyboardButton(translator.t("main.balance"))],
    ]
    if role == "admin":
        buttons.append([KeyboardButton(translator.t("main.admin"))])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)


def _permissions_from_value(value: str | None, *, ensure_income: bool = False) -> set[str]:
    """
    Преобразует строку разрешений в множество ("in", "out", "balance").
    ensure_income=True гарантирует присутствие "in" в результате.
    """
    if not value:
        perms: set[str] = set()
    else:
        tokens = {chunk.strip().lower() for chunk in value.split(",") if chunk.strip()}
        if "full" in tokens:
            perms = {"in", "out", "balance"}
        else:
            perms = {token for token in tokens if token in {"in", "out", "balance"}}
    if not perms:
        perms = {"in"}
    if ensure_income:
        perms.add("in")
    return perms


def _permissions_string_from_set(perms: set[str]) -> str:
    ordered = []
    for key in ("in", "out", "balance"):
        if key in perms:
            ordered.append(key)
    if not ordered:
        ordered.append("in")
    return ",".join(ordered)


def _flows_to_payments_label(perms: set[str], translator: Translator) -> str:
    flows = {p for p in perms if p in {"in", "out"}} or {"in"}
    if "in" in flows and "out" in flows:
        return translator.t("flows.in_out")
    if "out" in flows and "in" not in flows:
        return translator.t("flows.out")
    return translator.t("flows.in")


def _permissions_to_short_label(perms: set[str], translator: Translator) -> str:
    perms = perms or {"in"}
    has_in = "in" in perms
    has_out = "out" in perms
    has_balance = "balance" in perms
    parts = []
    if has_in and has_out:
        parts.append(translator.t("permissions.short.all"))
    elif has_out and not has_in:
        parts.append(translator.t("permissions.short.out"))
    else:
        parts.append(translator.t("permissions.short.in"))
    if has_balance:
        parts.append(translator.t("permissions.short.balance"))
    return ", ".join(parts)


def _attach_access_metadata(account: Dict[str, Any], perms: set[str]) -> Dict[str, Any]:
    acc = dict(account)
    acc_perms = set(perms) or {"in"}
    acc["access_permissions"] = acc_perms
    acc["permissions"] = _permissions_string_from_set(acc_perms)
    return acc


def _user_display_name(user_row: Dict[str, Any]) -> str:
    return (
        (user_row.get("friendly_name") or "").strip()
        or (user_row.get("full_name") or "").strip()
        or (user_row.get("username") or "").strip()
        or str(user_row.get("id", "пользователь"))
    )


def _parse_iso_date(token: str) -> date | None:
    try:
        return datetime.fromisoformat(token).date()
    except ValueError:
        return None


def _days_in_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def parse_custom_period_input(raw_text: str, *, now: datetime | None = None) -> Tuple[str, str]:
    """
    Поддерживает форматы:
      - "YYYY-MM-DD YYYY-MM-DD"
      - "DD DD" (дни текущего месяца, при day1>day2 -> начало в предыдущем месяце)
      - "DD" (один день текущего месяца)
    Возвращает кортеж (from_date_iso, to_date_iso).
    """
    text = (raw_text or "").strip()
    if not text:
        raise ValueError("empty input")

    now = now or datetime.now()
    today = now.date()
    parts = text.replace(",", " ").split()

    def parse_day_token(token: str) -> int | None:
        if token.isdigit() and 1 <= len(token) <= 2:
            return int(token)
        return None

    if len(parts) == 1:
        token = parts[0]
        iso = _parse_iso_date(token)
        if iso:
            return iso.isoformat(), iso.isoformat()

        day = parse_day_token(token)
        if day is None:
            raise ValueError("invalid single token")
        last_day = _days_in_month(today.year, today.month)
        if not (1 <= day <= last_day):
            raise ValueError("day out of range")
        single_date = date(today.year, today.month, day)
        return single_date.isoformat(), single_date.isoformat()

    if len(parts) == 2:
        iso_dates = [_parse_iso_date(token) for token in parts]
        if iso_dates[0] and iso_dates[1]:
            start, end = iso_dates
            if start > end:
                start, end = end, start
            return start.isoformat(), end.isoformat()

        day1 = parse_day_token(parts[0])
        day2 = parse_day_token(parts[1])
        if day1 is None or day2 is None:
            raise ValueError("invalid day tokens")

        year_to, month_to = today.year, today.month
        if day1 > day2:
            if month_to == 1:
                year_from, month_from = year_to - 1, 12
            else:
                year_from, month_from = year_to, month_to - 1
        else:
            year_from, month_from = year_to, month_to

        if day1 > _days_in_month(year_from, month_from):
            raise ValueError("start day out of range")
        if day2 > _days_in_month(year_to, month_to):
            raise ValueError("end day out of range")

        start_date = date(year_from, month_from, day1)
        end_date = date(year_to, month_to, day2)
        return start_date.isoformat(), end_date.isoformat()

    raise ValueError("too many tokens")


def user_allowed_for_menu(user_row: Dict[str, Any]) -> bool:
    return user_row["role"] in ("manager", "accountant", "admin")


def user_has_unlimited_days(user_row: Dict[str, Any]) -> bool:
    if user_row["role"] in ("admin", "accountant"):
        return True
    return user_row["max_days"] <= 0


def get_available_accounts_for_user(user_row: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Для admin: все активные счета (всегда с входящими, исходящие только если заданы).
    Для остальных (включая бухгалтера): только явно выданные счета.
    """
    role = user_row["role"]
    user_id = user_row["id"]

    if role == "admin":
        perm_map = get_user_account_permissions_map(user_id)
        accounts = list_all_active_accounts()
        result = []
        for acc in accounts:
            flows = _permissions_from_value(perm_map.get(acc["id"]))
            result.append(_attach_access_metadata(acc, flows))
        return result

    accounts = get_accounts_for_user(user_id)
    result = []
    for acc in accounts:
        flows = _permissions_from_value(acc.get("permissions"))
        result.append(_attach_access_metadata(acc, flows))
    return result


def get_statement_wait_left(context: ContextTypes.DEFAULT_TYPE, token: str) -> int:
    """
    Возвращает, сколько секунд ещё нужно подождать перед следующей выпиской
    по данному токену. 0 = можно вызывать сразу.
    НИЧЕГО не обновляет.
    """
    bot_data = context.application.bot_data
    key = f"last_statement_call_ts:{token}"

    last_ts = bot_data.get(key)
    if last_ts is None:
        return 0

    now = time.time()
    elapsed = now - last_ts
    if elapsed >= STATEMENT_MIN_INTERVAL:
        return 0

    return int(STATEMENT_MIN_INTERVAL - elapsed)


def mark_statement_call(context: ContextTypes.DEFAULT_TYPE, token: str) -> None:
    """
    Отмечает, что по этому токену только что делали вызов выписки.
    Вызывать ТОЛЬКО после успешного fetch_statement.
    """
    bot_data = context.application.bot_data
    key = f"last_statement_call_ts:{token}"
    bot_data[key] = time.time()


async def _reply(source, text: str):
    """
    Универсальный ответ:
    - Update.message
    - CallbackQuery.message
    - Message
    """
    if isinstance(source, Update):
        if source.message:
            await source.message.reply_text(text)
        elif source.callback_query and source.callback_query.message:
            await source.callback_query.message.reply_text(text)
        return

    if isinstance(source, CallbackQuery):
        if source.message:
            await source.message.reply_text(text)
        return

    if isinstance(source, Message):
        await source.reply_text(text)
        return

    if hasattr(source, "message") and source.message:
        await source.message.reply_text(text)
        return

    logging.warning("Unsupported source passed to _reply: %r", type(source))


# --- /start ---


async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_user = update.effective_user
    user_id = tg_user.id

    row = upsert_user_on_start(
        user_id=user_id,
        full_name=tg_user.full_name or "",
        username=tg_user.username or "",
    )

    translator = get_translator_for_user(row)

    display_name = _user_display_name(row)

    if row["role"] == "admin":
        await update.message.reply_text(
            translator.t("start.greeting_admin", name=display_name),
            reply_markup=build_main_menu("admin", translator),
        )
        return

    if row["role"] in ("manager", "accountant"):
        await update.message.reply_text(
            translator.t("start.greeting_user", name=display_name),
            reply_markup=build_main_menu(row["role"], translator),
        )
        return

    if row["role"] == "blocked":
        await update.message.reply_text(
            translator.t("start.blocked")
        )
        return
    # pending
    await update.message.reply_text(
        translator.t("start.pending")
    )

    # уведомить всех админов
    admin_ids = list_admin_ids()
    if not admin_ids:
        return

    text = (
        "Новый пользователь хочет доступ:\n"
        f"ID: {user_id}\n"
        f"Username: @{tg_user.username}\n"
        f"Имя: {tg_user.full_name}\n\n"
        "Выберите роль:"
    )
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "✅ Менеджер",
                    callback_data=f"approve:manager:{user_id}",
                ),
                InlineKeyboardButton(
                    "📊 Бухгалтер",
                    callback_data=f"approve:accountant:{user_id}",
                ),
            ],
            [
                InlineKeyboardButton(
                    "🛑 Заблокировать",
                    callback_data=f"approve:blocked:{user_id}",
                ),
            ],
        ]
    )

    for admin_id in admin_ids:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=text,
                reply_markup=keyboard,
            )
        except Exception:
            pass


# --- Админ: управление счетами пользователя ---


ADMIN_USER_ACCOUNTS_PREFIX = "admin_user_accounts"  # открыть меню счетов пользователя
ADMIN_USER_ACCOUNTS_ADD_PREFIX = "admin_user_accounts_add"  # выбор счета для добавления
ADMIN_USER_ACCOUNTS_DEL_PREFIX = "admin_user_accounts_del"  # выбор счета для удаления
ADMIN_USER_ACCOUNTS_PERM_PREFIX = (
    "admin_user_accounts_perm"  # настройки уровня доступа
)


async def admin_user_accounts_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    translator, _ = _translator_from_update(update)

    data = query.data  # формат "admin_user_accounts:<user_id>"
    _, user_id_str = data.split(":", 1)
    user_id = int(user_id_str)

    user = get_user(user_id)
    if not user:
        await query.edit_message_text(translator.t("Пользователь не найден."))
        return

    user_accounts = get_accounts_for_user(user_id)  # счета, доступные этому юзеру

    lines: list[str] = [
        translator.t("Пользователь: {name}", name=_user_display_name(user)),
        "",
        translator.t("Доступные счета:"),
    ]

    if not user_accounts:
        lines.append(translator.t("  — нет ни одного счета"))
    else:
        for acc in user_accounts:
            org = get_organization_by_id(acc["organization_id"])
            org_name = org["name"] if org else "?"
            perm_label = _permissions_to_short_label(
                _permissions_from_value(acc.get("permissions")), translator
            )
            lines.append(
                translator.t(
                    "  • {org} – {account} (уровень: {perm})",
                    org=org_name,
                    account=acc["name"],
                    perm=perm_label,
                )
            )

    text = "\n".join(lines)

    keyboard = [
        [
            InlineKeyboardButton(
                translator.t("➕ Добавить счёт"),
                callback_data=f"{ADMIN_USER_ACCOUNTS_ADD_PREFIX}:{user_id}",
            ),
        ],
        [
            InlineKeyboardButton(
                translator.t("➖ Удалить счёт"),
                callback_data=f"{ADMIN_USER_ACCOUNTS_DEL_PREFIX}:{user_id}",
            ),
        ],
        [
            InlineKeyboardButton(
                translator.t("⚙️ Уровень доступа"),
                callback_data=f"{ADMIN_USER_ACCOUNTS_PERM_PREFIX}:{user_id}",
            ),
        ],
        [
            InlineKeyboardButton(
                translator.t("⬅️ Назад"), callback_data=f"admin:user:{user_id}"
            ),
        ],
    ]

    await query.edit_message_text(
        text=text,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def admin_user_accounts_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    translator, _ = _translator_from_update(update)

    data = query.data  # "admin_user_accounts_add:<user_id>" или "...:<user_id>:<account_id>"
    parts = data.split(":")
    if len(parts) == 2:
        # шаг 1: показать список счетов для добавления
        _, user_id_str = parts
        user_id = int(user_id_str)

        user_accounts = get_accounts_for_user(user_id)
        all_accounts = list_all_active_accounts()

        user_acc_ids = {acc["id"] for acc in user_accounts}

        # счета, которых у пользователя ещё нет
        candidates = [acc for acc in all_accounts if acc["id"] not in user_acc_ids]

        if not candidates:
            await query.edit_message_text(
                translator.t("У пользователя уже есть доступ ко всем счетам."),
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                translator.t("⬅️ Назад"),
                                callback_data=f"{ADMIN_USER_ACCOUNTS_PREFIX}:{user_id}",
                            )
                        ]
                    ]
                ),
            )
            return

        keyboard_rows = []
        for acc in candidates:
            org = get_organization_by_id(acc["organization_id"])
            org_name = org["name"] if org else "?"
            label = translator.t("{org} – {account}", org=org_name, account=acc["name"])
            keyboard_rows.append(
                [
                    InlineKeyboardButton(
                        label,
                        callback_data=f"{ADMIN_USER_ACCOUNTS_ADD_PREFIX}:{user_id}:{acc['id']}",
                    )
                ]
            )

        keyboard_rows.append(
            [
                InlineKeyboardButton(
                    translator.t("⬅️ Назад"),
                    callback_data=f"{ADMIN_USER_ACCOUNTS_PREFIX}:{user_id}",
                )
            ]
        )

        await query.edit_message_text(
            text=translator.t("Выберите счёт, который нужно добавить пользователю:"),
            reply_markup=InlineKeyboardMarkup(keyboard_rows),
        )

    elif len(parts) == 3:
        # шаг 2: реально добавляем счёт
        _, user_id_str, acc_id_str = parts
        user_id = int(user_id_str)
        account_id = int(acc_id_str)

        grant_account_to_user(user_id, account_id)

        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        translator.t("⬅️ Назад"),
                        callback_data=f"{ADMIN_USER_ACCOUNTS_PREFIX}:{user_id}",
                    )
                ]
            ]
        )
        await query.edit_message_text(
            translator.t("Счёт добавлен пользователю."), reply_markup=keyboard
        )


async def admin_user_accounts_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    translator, _ = _translator_from_update(update)

    data = query.data  # "admin_user_accounts_del:<user_id>" или "...:<user_id>:<account_id>"
    parts = data.split(":")
    if len(parts) == 2:
        # шаг 1: показать список счетов для удаления
        _, user_id_str = parts
        user_id = int(user_id_str)

        user_accounts = get_accounts_for_user(user_id)

        if not user_accounts:
            await query.edit_message_text(
                translator.t("У пользователя нет счетов для удаления."),
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                translator.t("⬅️ Назад"),
                                callback_data=f"{ADMIN_USER_ACCOUNTS_PREFIX}:{user_id}",
                            )
                        ]
                    ]
                ),
            )
            return

        keyboard_rows = []
        for acc in user_accounts:
            org = get_organization_by_id(acc["organization_id"])
            org_name = org["name"] if org else "?"
            label = translator.t("{org} – {account}", org=org_name, account=acc["name"])
            keyboard_rows.append(
                [
                    InlineKeyboardButton(
                        label,
                        callback_data=f"{ADMIN_USER_ACCOUNTS_DEL_PREFIX}:{user_id}:{acc['id']}",
                    )
                ]
            )

        keyboard_rows.append(
            [
                InlineKeyboardButton(
                    translator.t("⬅️ Назад"),
                    callback_data=f"{ADMIN_USER_ACCOUNTS_PREFIX}:{user_id}",
                )
            ]
        )

        await query.edit_message_text(
            text=translator.t("Выберите счёт, который нужно удалить у пользователя:"),
            reply_markup=InlineKeyboardMarkup(keyboard_rows),
        )

    elif len(parts) == 3:
        # шаг 2: реально удаляем счёт
        _, user_id_str, acc_id_str = parts
        user_id = int(user_id_str)
        account_id = int(acc_id_str)

        revoke_account_from_user(user_id, account_id)

        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        translator.t("⬅️ Назад"),
                        callback_data=f"{ADMIN_USER_ACCOUNTS_PREFIX}:{user_id}",
                    )
                ]
            ]
        )
        await query.edit_message_text(
            translator.t("Счёт удалён у пользователя."), reply_markup=keyboard
        )



async def admin_user_accounts_perm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    translator, _ = _translator_from_update(update)

    parts = query.data.split(":")
    if len(parts) < 2:
        await query.edit_message_text(translator.t("Некорректный запрос."))
        return

    user_id = int(parts[1])
    user = get_user(user_id)
    if not user:
        await query.edit_message_text(translator.t("Пользователь не найден."))
        return

    if len(parts) == 2:
        user_accounts = get_accounts_for_user(user_id)
        if not user_accounts:
            await query.edit_message_text(
                translator.t("У пользователя нет привязанных счетов."),
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                translator.t("⬅️ Назад"),
                                callback_data=f"{ADMIN_USER_ACCOUNTS_PREFIX}:{user_id}",
                            )
                        ]
                    ]
                ),
            )
            return

        keyboard_rows = []
        for acc in user_accounts:
            org = get_organization_by_id(acc["organization_id"])
            org_name = org["name"] if org else "?"
            perm_label = _permissions_to_short_label(
                _permissions_from_value(acc.get("permissions")), translator
            )
            keyboard_rows.append(
                [
                    InlineKeyboardButton(
                        translator.t(
                            "{org} – {account} ({perm})",
                            org=org_name,
                            account=acc["name"],
                            perm=perm_label,
                        ),
                        callback_data=f"{ADMIN_USER_ACCOUNTS_PERM_PREFIX}:{user_id}:{acc['id']}",
                    )
                ]
            )

        keyboard_rows.append(
            [
                InlineKeyboardButton(
                    translator.t("⬅️ Назад"),
                    callback_data=f"{ADMIN_USER_ACCOUNTS_PREFIX}:{user_id}",
                )
            ]
        )

        await query.edit_message_text(
            translator.t("Выберите счёт для изменения уровня доступа:"),
            reply_markup=InlineKeyboardMarkup(keyboard_rows),
        )
        return

    account_id = int(parts[2])
    acc = get_account_by_id(account_id)
    if not acc:
        await query.edit_message_text(translator.t("errors.account_not_found"))
        return
    org = get_organization_by_id(acc["organization_id"])
    org_name = org["name"] if org else "?"

    available_tokens = (
        ("in", translator.t("permissions.payments.in")),
        ("out", translator.t("permissions.payments.out")),
        ("balance", translator.t("permissions.payments.balance")),
    )

    current_perms = _permissions_from_value(acc.get("permissions"))
    current_label = _permissions_to_short_label(current_perms, translator)
    base_text = translator.t(
        "permissions.title", account=f"{org_name} – {acc['name']}", current=current_label
    )

    if len(parts) == 3:
        missing = [token for token, _ in available_tokens if token not in current_perms]
        existing = [token for token, _ in available_tokens if token in current_perms]

        keyboard: list[list[InlineKeyboardButton]] = []
        if missing:
            keyboard.append(
                [
                    InlineKeyboardButton(
                        translator.t("permissions.add"),
                        callback_data=f"{ADMIN_USER_ACCOUNTS_PERM_PREFIX}:{user_id}:{account_id}:add",
                    )
                ]
            )
        if existing:
            keyboard.append(
                [
                    InlineKeyboardButton(
                        translator.t("permissions.remove"),
                        callback_data=f"{ADMIN_USER_ACCOUNTS_PERM_PREFIX}:{user_id}:{account_id}:del",
                    )
                ]
            )
        keyboard.append(
            [
                InlineKeyboardButton(
                    translator.t("⬅️ Назад"),
                    callback_data=f"{ADMIN_USER_ACCOUNTS_PREFIX}:{user_id}",
                )
            ]
        )

        await query.edit_message_text(
            base_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    action = parts[3]
    if action == "add":
        missing = [(t, label) for t, label in available_tokens if t not in current_perms]
        if len(parts) == 4:
            if not missing:
                await query.edit_message_text(
                    base_text,
                    reply_markup=InlineKeyboardMarkup(
                        [
                            [
                                InlineKeyboardButton(
                                    translator.t("⬅️ Назад"),
                                    callback_data=f"{ADMIN_USER_ACCOUNTS_PERM_PREFIX}:{user_id}:{account_id}",
                                )
                            ]
                        ]
                    ),
                )
                return
            keyboard = [
                [
                    InlineKeyboardButton(
                        label,
                        callback_data=f"{ADMIN_USER_ACCOUNTS_PERM_PREFIX}:{user_id}:{account_id}:add:{token}",
                    )
                ]
                for token, label in missing
            ]
            keyboard.append(
                [
                    InlineKeyboardButton(
                        translator.t("⬅️ Назад"),
                        callback_data=f"{ADMIN_USER_ACCOUNTS_PERM_PREFIX}:{user_id}:{account_id}",
                    )
                ]
            )
            await query.edit_message_text(
                translator.t("permissions.add.list"),
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        if len(parts) >= 5:
            token = parts[4]
            new_perms = set(current_perms)
            new_perms.add(token)
            updated = _permissions_string_from_set(new_perms)
            success = update_user_account_permissions(user_id, account_id, updated)
            if not success:
                await query.edit_message_text(translator.t("permissions.update_failed"))
                return
            label = _permissions_to_short_label(new_perms, translator)
            await query.edit_message_text(
                translator.t("permissions.updated", level=label),
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "⬅️ Назад",
                                callback_data=f"{ADMIN_USER_ACCOUNTS_PERM_PREFIX}:{user_id}:{account_id}",
                            )
                        ]
                    ]
                ),
            )
            return

    if action == "del":
        existing = [(t, label) for t, label in available_tokens if t in current_perms]
        if len(parts) == 4:
            if not existing:
                await query.edit_message_text(
                    base_text,
                    reply_markup=InlineKeyboardMarkup(
                        [
                            [
                                InlineKeyboardButton(
                                    translator.t("⬅️ Назад"),
                                    callback_data=f"{ADMIN_USER_ACCOUNTS_PERM_PREFIX}:{user_id}:{account_id}",
                                )
                            ]
                        ]
                    ),
                )
                return
            keyboard = [
                [
                    InlineKeyboardButton(
                        label,
                        callback_data=f"{ADMIN_USER_ACCOUNTS_PERM_PREFIX}:{user_id}:{account_id}:del:{token}",
                    )
                ]
                for token, label in existing
            ]
            keyboard.append(
                [
                    InlineKeyboardButton(
                        translator.t("⬅️ Назад"),
                        callback_data=f"{ADMIN_USER_ACCOUNTS_PERM_PREFIX}:{user_id}:{account_id}",
                    )
                ]
            )
            await query.edit_message_text(
                translator.t("permissions.remove.list"),
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            return

        if len(parts) >= 5:
            token = parts[4]
            new_perms = {p for p in current_perms if p != token}
            updated = _permissions_string_from_set(new_perms)
            success = update_user_account_permissions(user_id, account_id, updated)
            if not success:
                await query.edit_message_text(translator.t("permissions.update_failed"))
                return
            label = _permissions_to_short_label(new_perms, translator)
            await query.edit_message_text(
                translator.t("permissions.updated", level=label),
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                translator.t("⬅️ Назад"),
                                callback_data=f"{ADMIN_USER_ACCOUNTS_PERM_PREFIX}:{user_id}:{account_id}",
                            )
                        ]
                    ]
                ),
            )
            return

    await query.edit_message_text(translator.t("Некорректный запрос."))
# --- approve от админа ---


async def approve_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработка кнопок одобрения нового пользователя:
    callback_data: 'approve:<role>:<user_id>'
    """
    query = update.callback_query
    await query.answer()

    data = query.data
    try:
        prefix, role, uid_str = data.split(":")
        uid = int(uid_str)
    except Exception:
        await query.edit_message_text("Некорректные данные approve callback.")
        return

    # Проверяем, что нажимающий — админ
    from_user = update.effective_user
    if not is_admin(from_user.id):
        await query.edit_message_text("⛔ Только администратор может одобрять пользователей.")
        return

    if role in ("manager", "accountant", "admin"):
        if role == "manager":
            suggested = 7
        else:
            suggested = 0
        context.user_data["admin_mode"] = "approve_set_friendly_name"
        context.user_data["pending_user_setup"] = {
            "target_id": uid,
            "role": role,
            "suggested_max_days": suggested,
        }
        await query.edit_message_text(
            "Введите friendly name (понятное имя) для нового пользователя:",
            parse_mode="Markdown",
        )
        return

    # Для остальных ролей (blocked/pending) оставляем старую логику
    if role == "pending":
        max_days = 3
    else:
        max_days = 0

    update_user_role(uid, role, max_days=max_days)

    u = get_user(uid)
    uname = ""
    if u and u.get("username"):
        uname = f"@{u['username']}"

    await query.edit_message_text(
        f"✅ Роль пользователя {uid} {uname} установлена: `{role}`.",
        parse_mode="Markdown",
    )

    try:
        from telegram import ReplyKeyboardRemove

        if role == "blocked":
            txt = "⛔ Вам отказано в доступе к боту. Обратитесь к администратору."
            await context.bot.send_message(
                chat_id=uid,
                text=txt,
                reply_markup=ReplyKeyboardRemove(),
            )
        elif role == "pending":
            txt = "Ваш статус в боте: pending. Ожидайте решения администратора."
            await context.bot.send_message(chat_id=uid, text=txt)
    except Exception:
        pass


# --- Админ-меню ---


async def admin_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработка нажатий в админ-меню (callback_data начинается с 'admin:').
    """
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    if not is_admin(user_id):
        await query.edit_message_text("⛔ Только администратор может пользоваться этим меню.")
        return

    data = query.data
    parts = data.split(":")
    if len(parts) < 2:
        await query.edit_message_text("Некорректные данные admin callback.")
        return

    action = parts[1]

    # --- Добавление организации ---
    if action == "add_org":
        context.user_data["admin_mode"] = "add_org_name"
        context.user_data.pop("new_org_name", None)
        await query.edit_message_text(
            "Введите *имя организации* (как оно будет отображаться в отчётах):",
            parse_mode="Markdown",
        )
        return

    # --- Работа со счетами: выбор организации ---
    if action == "accounts":
        orgs = list_organizations()
        if not orgs:
            await query.edit_message_text(
                "Пока нет ни одной организации. Сначала добавьте организацию."
            )
            return

        keyboard = []
        for org in orgs:
            keyboard.append(
                [
                    InlineKeyboardButton(
                        f"🏢 {org['name']}",
                        callback_data=f"admin:acc_org:{org['id']}",
                    )
                ]
            )

        await query.edit_message_text(
            "Выберите организацию для работы со счетами:",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    # --- Список пользователей ---
    if action == "users":
        users = list_users()
        if not users:
            await query.edit_message_text("Пользователей пока нет.")
            return

        keyboard = []
        for u in users:
            role = u["role"]
            if role == "admin":
                role_icon = "👑"
            elif role == "accountant":
                role_icon = "📊"
            elif role == "manager":
                role_icon = "👔"
            elif role == "pending":
                role_icon = "👤"
            elif role == "blocked":
                role_icon = "⛔"
            else:
                role_icon = "❓"

            display_name = _user_display_name(u)
            uname = f" (@{u['username']})" if u.get("username") else ""
            label = f"{role_icon} {display_name}{uname} – ID {u['id']}"

            keyboard.append(
                [
                    InlineKeyboardButton(
                        label,
                        callback_data=f"admin:user:{u['id']}",
                    )
                ]
            )

        await query.edit_message_text(
            "👥 Список пользователей:\n"
            "Выберите пользователя, чтобы изменить его роль или права по счетам.",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    # --- дальше нужны ID ---
    if action in (
        "acc_org",
        "acc_add",
        "acc_add_select",
        "acc_list",
        "acc_info",
        "user",
        "user_roles",
        "user_fname",
        "user_maxdays",
    ):
        if len(parts) < 3:
            await query.edit_message_text(
                "Некорректные данные admin callback (ожидается ID)."
            )
            return
        try:
            obj_id = int(parts[2])
        except ValueError:
            await query.edit_message_text("Некорректный ID в admin callback.")
            return
    else:
        obj_id = None

    # --- Подменю по организации ---
    if action == "acc_org":
        org = get_organization_by_id(obj_id)
        if not org:
            await query.edit_message_text("Организация не найдена.")
            return

        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "➕ Добавить счёт",
                        callback_data=f"admin:acc_add:{org['id']}",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        "📋 Список счетов",
                        callback_data=f"admin:acc_list:{org['id']}",
                    ),
                ],
            ]
        )

        await query.edit_message_text(
            f"Организация: *{org['name']}*\nВыберите действие:",
            parse_mode="Markdown",
            reply_markup=keyboard,
        )
        return

    # --- Запуск диалога добавления счёта ---
    if action == "acc_add":
        org = get_organization_by_id(obj_id)
        if not org:
            await query.edit_message_text("Организация не найдена.")
            return

        token = org.get("token")
        if not token:
            await query.edit_message_text(
                "У выбранной организации не задан токен Monobank. Сначала добавьте токен."
            )
            return

        try:
            client_info = fetch_client_info(token)
        except HTTPError as e:
            await query.edit_message_text(
                "Не удалось получить список счетов из Monobank "
                f"(HTTP {e.response.status_code if e.response else '??'})."
            )
            return
        except Exception as exc:
            logging.exception("Failed to fetch client info for org %s", org["id"])
            await query.edit_message_text(
                "Не удалось получить список счетов из Monobank. Попробуйте позже."
            )
            return

        api_accounts = client_info.get("accounts") or []
        existing = {
            acc["mono_account_id"]
            for acc in list_accounts_by_org(org["id"])
            if acc.get("mono_account_id")
        }

        options: list[dict[str, Any]] = []
        for idx, api_acc in enumerate(api_accounts, start=1):
            mono_id = api_acc.get("id")
            iban = (api_acc.get("iban") or "").strip()
            if not mono_id or not iban:
                continue
            if mono_id in existing:
                continue
            currency_code = api_acc.get("currencyCode")
            options.append(
                {
                    "option_id": str(idx),
                    "mono_account_id": mono_id,
                    "iban": iban,
                    "currency_code": currency_code,
                    "raw": api_acc,
                }
            )

        if not options:
            await query.edit_message_text(
                "Для этой организации нет новых счетов с IBAN, которые можно добавить."
            )
            return

        option_map = {opt["option_id"]: opt for opt in options}
        context.user_data["acc_add_state"] = {
            "org_id": org["id"],
            "org_name": org["name"],
            "options": option_map,
        }

        keyboard_rows = []
        for opt in options:
            currency_code = opt["currency_code"]
            currency_label = f"{currency_code}" if currency_code else "?"
            label = f"{opt['iban']} — {currency_label}"
            keyboard_rows.append(
                [
                    InlineKeyboardButton(
                        label,
                        callback_data=f"admin:acc_add_select:{org['id']}:{opt['option_id']}",
                    )
                ]
            )

        keyboard_rows.append(
            [
                InlineKeyboardButton(
                    "⬅️ Назад",
                    callback_data=f"admin:acc_org:{org['id']}",
                )
            ]
        )

        await query.edit_message_text(
            f"Организация: *{org['name']}*\nВыберите счёт (IBAN) из Monobank:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard_rows),
        )
        return

    # --- Список счетов по организации ---
    if action == "acc_list":
        org = get_organization_by_id(obj_id)
        if not org:
            await query.edit_message_text("Организация не найдена.")
            return

        accounts = list_accounts_by_org(org["id"])
        if not accounts:
            await query.edit_message_text(
                f"У организации *{org['name']}* пока нет ни одной карты.",
                parse_mode="Markdown",
            )
            return

        keyboard = []
        for acc in accounts:
            keyboard.append(
                [
                    InlineKeyboardButton(
                        f"💳 {acc['name']}",
                        callback_data=f"admin:acc_info:{acc['id']}",
                    )
                ]
            )

        await query.edit_message_text(
            f"Карты организации *{org['name']}*:\n"
            "Выберите карту, чтобы посмотреть подробную информацию.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if action == "acc_add_select":
        if len(parts) < 4:
            await query.edit_message_text("Некорректные данные для выбора счёта.")
            return

        option_id = parts[3]
        state = context.user_data.get("acc_add_state") or {}
        if state.get("org_id") != obj_id:
            await query.edit_message_text(
                "Данные по выбранной организации устарели. Начните добавление заново."
            )
            return

        option = (state.get("options") or {}).get(option_id)
        if not option:
            await query.edit_message_text(
                "Этот счёт больше недоступен. Попробуйте выбрать снова."
            )
            return

        context.user_data["admin_mode"] = "add_account_name"
        context.user_data["acc_org_id"] = obj_id
        context.user_data["acc_mono_id"] = option["mono_account_id"]
        context.user_data["acc_iban"] = option["iban"]
        context.user_data["acc_currency_code"] = option.get("currency_code")
        context.user_data["acc_add_state_option"] = option
        context.user_data["acc_add_state_org_name"] = state.get("org_name")

        await query.edit_message_text(
            f"Организация: *{state.get('org_name', '?')}*\n"
            f"IBAN: `{option['iban']}`\n\n"
            "Введите *имя счёта*, под которым он будет отображаться:",
            parse_mode="Markdown",
        )
        return

    # --- Подробная информация по карте ---
    if action == "acc_info":
        acc = get_account_by_id(obj_id)
        if not acc:
            await query.edit_message_text("Карта не найдена.")
            return

        org = get_organization_by_id(acc["organization_id"])
        org_name = org["name"] if org else "(неизвестно)"

        text = (
            f"💳 *Карта:* {acc['name']}\n"
            f"🏢 Организация: {org_name}\n"
            f"ID карты (в БД): `{acc['id']}`\n"
            f"Monobank account id: `{acc['mono_account_id']}`\n"
            f"IBAN: `{acc['iban'] or ''}`\n"
            f"Код валюты: `{acc['currency_code'] or ''}`\n"
            f"Активна: {'✅' if acc['is_active'] else '❌'}"
        )

        await query.edit_message_text(
            text,
            parse_mode="Markdown",
        )
        return

    # --- Карточка пользователя ---
    if action == "user":
        u = get_user(obj_id)
        if not u:
            await query.edit_message_text("Пользователь не найден.")
            return

        role = u["role"]
        max_days = u["max_days"]

        uname = f"@{u['username']}" if u["username"] else "(нет username)"
        friendly = u.get("friendly_name") or "—"
        text = (
            f"👤 Пользователь: *{_user_display_name(u)}*\n"
            f"ID: `{u['id']}`\n"
            f"Username: {uname}\n"
            f"Friendly name: {friendly}\n"
            f"Роль: `{role}`\n"
            f"MaxDays: {max_days}\n\n"
            "Выберите действие:"
        )

        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "👤 Изменить роль",
                        callback_data=f"admin:user_roles:{u['id']}",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        "💳 Счета пользователя",
                        callback_data=f"{ADMIN_USER_ACCOUNTS_PREFIX}:{u['id']}",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        "✏️ Friendly name",
                        callback_data=f"admin:user_fname:{u['id']}",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        "📆 Max days",
                        callback_data=f"admin:user_maxdays:{u['id']}",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ Назад к списку",
                        callback_data="admin:users",
                    ),
                ],
            ]
        )

        await query.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    # --- Подменю: список ролей пользователя ---
    
    if action == "user_fname":
        u = get_user(obj_id)
        if not u:
            await query.edit_message_text("Пользователь не найден.")
            return
        context.user_data["admin_mode"] = "edit_user_friendly_name"
        context.user_data["edit_user_target_id"] = obj_id
        await query.edit_message_text(
            f"Введите новое friendly name для пользователя {_user_display_name(u)}:",
            parse_mode="Markdown",
        )
        return

    if action == "user_maxdays":
        u = get_user(obj_id)
        if not u:
            await query.edit_message_text("Пользователь не найден.")
            return
        context.user_data["admin_mode"] = "edit_user_max_days"
        context.user_data["edit_user_target_id"] = obj_id
        await query.edit_message_text(
            f"Введите новое значение `max_days` для {_user_display_name(u)} "
            "(целое число, 0 = без ограничений):",
            parse_mode="Markdown",
        )
        return
    if action == "user_roles":
        u = get_user(obj_id)
        if not u:
            await query.edit_message_text("Пользователь не найден.")
            return

        current_role = u["role"]
        uname = f"@{u['username']}" if u["username"] else "(нет username)"
        text = (
            f"👤 Изменить роль\n\n"
            f"Пользователь: *{_user_display_name(u)}*\n"
            f"ID: `{u['id']}`\n"
            f"Username: {uname}\n"
            f"Текущая роль: `{current_role}`\n\n"
            "Выберите новую роль:"
        )

        def role_button(label: str, role_code: str) -> InlineKeyboardButton:
            return InlineKeyboardButton(
                label,
                callback_data=f"admin:userrole:{role_code}:{u['id']}",
            )

        # pending НЕ показываем, текущую роль НЕ показываем
        role_options = [
            ("👔 Менеджер", "manager"),
            ("📊 Бухгалтер", "accountant"),
            ("👑 Админ", "admin"),
            ("⛔ Blocked", "blocked"),
        ]

        rows: list[list[InlineKeyboardButton]] = []
        current_row: list[InlineKeyboardButton] = []

        for label, code in role_options:
            if code == current_role:
                continue  # не показываем текущую роль
            current_row.append(role_button(label, code))
            if len(current_row) == 2:
                rows.append(current_row)
                current_row = []

        if current_row:
            rows.append(current_row)

        rows.append(
            [
                InlineKeyboardButton(
                    "⬅️ Назад",
                    callback_data=f"admin:user:{u['id']}",
                )
            ]
        )

        kb = InlineKeyboardMarkup(rows)

        await query.edit_message_text(
            text,
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    # --- Смена роли пользователю ---
    if action == "userrole":
        if len(parts) < 4:
            await query.edit_message_text("Некорректные данные admin:userrole callback.")
            return

        new_role = parts[2]
        try:
            target_id = int(parts[3])
        except ValueError:
            await query.edit_message_text("Некорректный ID пользователя.")
            return

        # pending нельзя назначать вручную из меню
        if new_role == "pending":
            await query.edit_message_text(
                "Роль 'pending' назначается только автоматически и не может быть выбрана вручную."
            )
            return

        if new_role == "manager":
            max_days = 7
        elif new_role in ("accountant", "admin"):
            max_days = 0
        else:
            # blocked и любые другие
            max_days = 0

        update_user_role(target_id, new_role, max_days=max_days)

        u = get_user(target_id)
        uname = f"@{u['username']}" if u and u["username"] else ""

        await query.edit_message_text(
            f"✅ Роль пользователя {target_id} {uname} изменена на `{new_role}`.",
            parse_mode="Markdown",
        )

        # Пытаемся обновить меню у самого пользователя
        try:
            from telegram import ReplyKeyboardRemove

            txt = f"Ваша роль в боте изменена на: {new_role}."
            if new_role == "blocked":
                await query.bot.send_message(
                    chat_id=target_id,
                    text=txt,
                    reply_markup=ReplyKeyboardRemove(),
                )
            else:
                await query.bot.send_message(
                    chat_id=target_id,
                    text=txt,
                    reply_markup=build_main_menu(new_role),
                )
        except Exception:
            pass

        return

    await query.edit_message_text("Эта функция админ-меню ещё не реализована.")


# --- Guard для активного пользователя ---


async def ensure_active_user(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> Dict[str, Any] | None:
    tg_user = update.effective_user
    user_row = get_user(tg_user.id)
    translator = get_translator_for_user(user_row)
    if not user_row:
        await update.message.reply_text(translator.t("errors.use_start"))
        return None

    if user_row["role"] in ("pending",):
        await update.message.reply_text(translator.t("errors.pending"))
        return None

    if user_row["role"] == "blocked":
        await update.message.reply_text(translator.t("errors.blocked"))
        return None

    return user_row


# --- Платежи (текстовый вывод) ---


async def handle_payments_entry(
    update: Update, context: ContextTypes.DEFAULT_TYPE, user_row: Dict[str, Any]
):
    translator = get_translator_for_user(user_row)
    accounts = get_available_accounts_for_user(user_row)

    if not accounts:
        await update.message.reply_text(translator.t("payments.no_accounts"))
        return

    # Если только одна карта — сразу к выбору периода
    if len(accounts) == 1:
        acc = accounts[0]
        await ask_period_for_payments(update, context, user_row, str(acc["id"]))
        return

    # Несколько карт — меню "Все карты" + список карт
    keyboard = []

    keyboard.append(
        [
            InlineKeyboardButton(
                translator.t("payments.all_cards"),
                callback_data="pay_acc:all",
            )
        ]
    )

    for acc in accounts:
        org = get_organization_by_id(acc["organization_id"])
        org_name = org["name"] if org else "?"
        display_name = f"{org_name} – {acc['name']}"

        keyboard.append(
            [
                InlineKeyboardButton(
                    f"💳 {display_name}",
                    callback_data=f"pay_acc:{acc['id']}",
                )
            ]
        )

    await update.message.reply_text(
        translator.t("payments.choose_card"),
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def ask_period_for_payments(
    source, context: ContextTypes.DEFAULT_TYPE, user_row: Dict[str, Any], account_key: str
):
    """
    account_key: "all" или строковый id карты.
    """
    translator = get_translator_for_user(user_row)
    if account_key == "all":
        card_label = translator.t("payments.all_cards_label")
    else:
        try:
            acc_id = int(account_key)
        except ValueError:
            await _reply(source, translator.t("errors.invalid_card"))
            return
        available = get_available_accounts_for_user(user_row)
        acc = next((a for a in available if a["id"] == acc_id), None)
        if not acc:
            await _reply(source, translator.t("errors.card_unavailable"))
            return
        org = get_organization_by_id(acc["organization_id"])
        org_name = org["name"] if org else "?"
        card_label = f"{org_name} – {acc['name']}"

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    translator.t("payments.period.last_hour"),
                    callback_data=f"pay_per:{account_key}:last_hour",
                ),
                InlineKeyboardButton(
                    translator.t("payments.period.last_3_hours"),
                    callback_data=f"pay_per:{account_key}:last_3_hours",
                ),
            ],
            [
                InlineKeyboardButton(
                    translator.t("payments.period.today"),
                    callback_data=f"pay_per:{account_key}:today",
                ),
                InlineKeyboardButton(
                    translator.t("payments.period.yesterday"),
                    callback_data=f"pay_per:{account_key}:yesterday",
                ),
            ],
            [
                InlineKeyboardButton(
                    translator.t("payments.period.custom"),
                    callback_data=f"pay_per:{account_key}:custom",
                ),
            ],
        ]
    )

    text = translator.t("payments.period.title", {"card": card_label})
    if hasattr(source, "message") and source.message:
        await source.message.reply_text(
            text, reply_markup=keyboard, parse_mode="Markdown"
        )
    else:
        await source.edit_message_text(
            text, reply_markup=keyboard, parse_mode="Markdown"
        )
    context.user_data["pay_period_pending"] = account_key


async def handle_balance_entry(
    update: Update, context: ContextTypes.DEFAULT_TYPE, user_row: Dict[str, Any]
):
    translator = get_translator_for_user(user_row)
    accounts = get_available_accounts_for_user(user_row)
    allowed = [
        acc
        for acc in accounts
        if "balance" in (acc.get("access_permissions") or set())
    ]

    if not allowed:
        await _reply(update, "У вас нет доступа к балансу ни по одному счёту.")
        return

    by_org: Dict[int, list[Dict[str, Any]]] = {}
    for acc in allowed:
        by_org.setdefault(acc["organization_id"], []).append(acc)

    lines: list[str] = []
    for org_id, accs in by_org.items():
        org = get_organization_by_id(org_id)
        if not org or not org.get("is_active"):
            continue
        token = org.get("token")
        if not token:
            continue
        try:
            info = fetch_client_info(token)
        except HTTPError:
            await _reply(update, "Не удалось получить баланс по организациям.")
            return

        api_accounts = info.get("accounts") or []
        for acc in accs:
            api_match = next(
                (a for a in api_accounts if a.get("id") == acc.get("mono_account_id")),
                None,
            )
            if not api_match:
                continue
            balance_value = int(api_match.get("balance", 0)) / 100.0
            currency_code = api_match.get("currencyCode") or ""
            org_name = org.get("name") or "?"
            lines.append(
                f"{org_name} – {acc['name']}: {balance_value:.2f} {currency_code}"
            )

    if not lines:
        await _reply(update, "Не удалось найти данные по балансу.")
        return

    text = "\n".join(lines)
    await _reply(update, text)


async def pay_acc_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_row = get_user(query.from_user.id)
    translator = get_translator_for_user(user_row)
    if not user_row or not user_allowed_for_menu(user_row):
        await query.edit_message_text(translator.t("errors.no_access"))
        return

    _, acc_key = query.data.split(":")  # "all" или "<id>"

    await ask_period_for_payments(query, context, user_row, acc_key)


async def pay_period_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_row = get_user(query.from_user.id)
    translator = get_translator_for_user(user_row)
    if not user_row or not user_allowed_for_menu(user_row):
        await query.edit_message_text(translator.t("errors.no_access"))
        return

    # data: "pay_per:<account_key>:<mode>"
    _, acc_key, mode = query.data.split(":")
    context.user_data.pop("pay_period_pending", None)

    now = datetime.now()
    today = now.date()

    if mode == "last_hour":
        from_ts = int((now - timedelta(hours=1)).timestamp())
        to_ts = int(now.timestamp())
        await show_payments_for_period(query, context, user_row, acc_key, from_ts, to_ts)
        return
    if mode == "last_3_hours":
        from_ts = int((now - timedelta(hours=3)).timestamp())
        to_ts = int(now.timestamp())
        await show_payments_for_period(query, context, user_row, acc_key, from_ts, to_ts)
        return

    if mode == "today":
        from_raw = today.isoformat()
        to_raw = today.isoformat()
    elif mode == "yesterday":
        yest = today - timedelta(days=1)
        from_raw = to_raw = yest.isoformat()
    elif mode == "custom":
        context.user_data["pay_custom_acc_id"] = acc_key
        await query.edit_message_text(
            get_custom_period_help(translator),
            parse_mode="Markdown",
        )
        return
    else:
        return

    from_ts = unix_from_str(from_raw, is_to=False)
    to_ts = unix_from_str(to_raw, is_to=True)
    await show_payments_for_period(query, context, user_row, acc_key, from_ts, to_ts)


async def show_payments_for_period(
    source,
    context: ContextTypes.DEFAULT_TYPE,
    user_row: Dict[str, Any],
    account_key: str,
    from_ts: int,
    to_ts: int,
):
    """
    account_key: "all" или строковый id карты.
    Показывает приходные операции по одной карте или по всем доступным картам.
    """

    translator = get_translator_for_user(user_row)

    action_params = {
        "from": datetime.fromtimestamp(from_ts).isoformat(),
        "to": datetime.fromtimestamp(to_ts).isoformat(),
        "accounts": [],
    }

    def log_action(result: int, output: str) -> None:
        try:
            log_user_action(
                user_id=user_row["id"],
                action_name="payments",
                result=result,
                params=action_params,
                output=output,
            )
        except Exception:
            logging.exception("Failed to log payments action")

    # --- Проверка лимита по дням ---
    if not user_has_unlimited_days(user_row):
        days = (to_ts - from_ts) / 86400.0
        if days > user_row["max_days"] + 1e-6:
            await _reply(
                source,
                translator.t(
                    "errors.period_limit", {"days": user_row["max_days"]}
                ),
            )
            log_action(0, "Период превышает допустимый лимит")
            return

    ignore_ibans = get_ignore_ibans_norm()

    available_accounts = get_available_accounts_for_user(user_row)
    if account_key == "all":
        accounts = available_accounts
    else:
        try:
            acc_id = int(account_key)
        except ValueError:
            await _reply(source, translator.t("errors.invalid_card"))
            log_action(0, "Некорректный идентификатор карты")
            return
        accounts = [acc for acc in available_accounts if acc["id"] == acc_id]

    if not accounts:
        await _reply(source, translator.t("payments.no_available_cards"))
        log_action(0, "Нет доступных карт")
        return

    all_lines: list[str] = []
    total_ops = 0

    # --- Кеш организаций и сбор токенов ---
    org_cache: Dict[int, Dict[str, Any]] = {}
    tokens: set[str] = set()
    account_labels: list[str] = []

    for acc in accounts:
        org_id = acc.get("organization_id")
        if org_id is None:
            continue

        org = org_cache.get(org_id)
        if org is None:
            org = get_organization_by_id(org_id)
            org_cache[org_id] = org

        if not org or not org.get("is_active", True):
            continue

        token = org.get("token")
        if not token:
            continue

        tokens.add(token)

        org_name = org.get("name") if org else "?"
        account_labels.append(f"{org_name} – {acc['name']}")

    action_params["accounts"] = account_labels

    if not tokens:
        await _reply(
            source,
            translator.t("statement.no_active_tokens"),
        )
        log_action(0, "Нет активных организаций с токенами")
        return

    # --- Проверяем лимит Monobank по всем токенам ---
    max_wait_left = max(get_statement_wait_left(context, token) for token in tokens)
    if max_wait_left > 0:
        msg = translator.t("errors.monobank_rate_limit") + "\n"
        msg += translator.t("errors.monobank_retry_in", {"seconds": max_wait_left})
        await _reply(source, msg)
        log_action(0, msg)
        return

    # --- Основной цикл по аккаунтам ---
    prev_org_id: int | None = None
    first_block = True

    for acc in accounts:
        org_id = acc.get("organization_id")
        org = org_cache.get(org_id)
        if not org or not org.get("is_active", True):
            continue

        token = org.get("token")
        if not token:
            continue

        org_name = org.get("name") or "?"
        card_label = f"{org_name} – {acc['name']}"
        flows_allowed = acc.get("access_permissions") or {"in"}
        allow_in = "in" in flows_allowed
        allow_out = "out" in flows_allowed

        try:
            items = fetch_statement(token, acc["mono_account_id"], from_ts, to_ts)
            mark_statement_call(context, token)
        except HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait_left = get_statement_wait_left(context, token)
                msg = translator.t("errors.monobank_rate_limit") + "\n"
                if wait_left > 0:
                    msg += translator.t(
                        "errors.monobank_retry_in", {"seconds": wait_left}
                    )
                else:
                    msg += translator.t("errors.monobank_retry_later")
                await _reply(source, msg)
                log_action(0, msg)
                return
            raise

        filtered_items, included_flows = filter_income_and_ignore(
            items,
            ignore_ibans,
            allow_in=allow_in,
            allow_out=allow_out,
        )

        if not filtered_items:
            continue

        if not first_block:
            if prev_org_id != org_id:
                all_lines.append("")
                all_lines.append("")
            else:
                all_lines.append("")
        first_block = False
        prev_org_id = org_id

        header_label = _flows_to_payments_label(included_flows, translator)
        all_lines.append(f"💳 {card_label} — {header_label}")

        for it in sorted(filtered_items, key=lambda x: int(x.get("time", 0))):
            t = int(it.get("time", 0))
            dt_str = datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S")
            amount = int(it.get("amount", 0)) / 100.0
            flow = "out" if amount < 0 else "in"
            prefix = "🔴 -" if flow == "out" else "🟢 +"
            formatted_amount = f"{prefix}{abs(amount):.2f} UAH"
            comment = it.get("comment") or it.get("description") or ""
            line = f"{dt_str} — {formatted_amount}"
            all_lines.append(line)
            if comment:
                all_lines.append(f"  {comment}")
            total_ops += 1

    if total_ops == 0:
        msg = translator.t("payments.no_payments_period")
        await _reply(source, msg)
        log_action(0, msg)
        return

    text = "\n".join(all_lines)
    await _reply(source, text)
    log_action(1, text)


# --- Выписка (Excel) ---


async def ask_statement_period(
    source,
    context: ContextTypes.DEFAULT_TYPE,
    user_row: Dict[str, Any],
    account: Dict[str, Any] | None,
):
    """
    account:
      - None  → режим "Все карты"
      - dict  → конкретная карта
    """
    translator = get_translator_for_user(user_row)
    if account is None:
        label = translator.t("payments.all_cards_label")
    else:
        org = get_organization_by_id(account["organization_id"])
        org_name = org["name"] if org else "?"
        label = f"{org_name} – {account['name']}"

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    translator.t("statement.period.today"),
                    callback_data="stmt_per:today",
                ),
                InlineKeyboardButton(
                    translator.t("statement.period.yesterday"),
                    callback_data="stmt_per:yesterday",
                ),
            ],
            [
                InlineKeyboardButton(
                    translator.t("statement.period.last3"),
                    callback_data="stmt_per:last3",
                ),
            ],
            [
                InlineKeyboardButton(
                    translator.t("statement.period.custom"),
                    callback_data="stmt_per:custom",
                ),
            ],
        ]
    )

    text = translator.t("statement.period.title", {"card": label})
    if hasattr(source, "message") and source.message:
        await source.message.reply_text(
            text, reply_markup=keyboard, parse_mode="Markdown"
        )
    else:
        await source.edit_message_text(
            text, reply_markup=keyboard, parse_mode="Markdown"
        )
    account_key = context.user_data.get("stmt_account_key")
    if account_key is None:
        account_key = "all" if account is None else str(account["id"])
        context.user_data["stmt_account_key"] = account_key
    context.user_data["stmt_period_pending"] = account_key


async def handle_statement_entry(
    update: Update, context: ContextTypes.DEFAULT_TYPE, user_row: Dict[str, Any]
):
    translator = get_translator_for_user(user_row)
    accounts = get_available_accounts_for_user(user_row)

    if not accounts:
        await update.message.reply_text(
            translator.t("statement.no_accounts"),
        )
        return

    if len(accounts) == 1:
        acc = accounts[0]
        context.user_data["stmt_account_key"] = str(acc["id"])
        await ask_statement_period(update, context, user_row, acc)
        return

    keyboard = []

    keyboard.append(
        [InlineKeyboardButton(translator.t("statement.all_cards"), callback_data="stmt_acc:all")]
    )

    for acc in accounts:
        org = get_organization_by_id(acc["organization_id"])
        org_name = org["name"] if org else "?"
        display_name = f"{org_name} – {acc['name']}"
        keyboard.append(
            [
                InlineKeyboardButton(
                    f"💳 {display_name}",
                    callback_data=f"stmt_acc:{acc['id']}",
                )
            ]
        )

    await update.message.reply_text(
        translator.t("statement.choose_card"),
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def stmt_acc_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_row = get_user(query.from_user.id)
    translator = get_translator_for_user(user_row)
    if not user_row or not user_allowed_for_menu(user_row):
        await query.edit_message_text(translator.t("errors.no_access"))
        return

    _, acc_key = query.data.split(":")  # "all" или "<id>"

    context.user_data["stmt_account_key"] = acc_key

    available_accounts = get_available_accounts_for_user(user_row)

    if acc_key == "all":
        account = None
    else:
        try:
            acc_id = int(acc_key)
        except ValueError:
            await query.edit_message_text(translator.t("errors.invalid_card"))
            return
        account = next((a for a in available_accounts if a["id"] == acc_id), None)
        if not account:
            await query.edit_message_text(translator.t("errors.card_unavailable"))
            return

    await ask_statement_period(query, context, user_row, account)


async def stmt_period_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_row = get_user(query.from_user.id)
    translator = get_translator_for_user(user_row)
    if not user_row or not user_allowed_for_menu(user_row):
        await query.edit_message_text(translator.t("errors.no_access"))
        return

    account_key = context.user_data.get("stmt_account_key")
    if account_key is None:
        await query.edit_message_text(translator.t("statement.select_card_first"))
        return

    _, mode = query.data.split(":")
    context.user_data.pop("stmt_period_pending", None)

    now = datetime.now()
    today = now.date()

    if mode == "today":
        from_raw = today.isoformat()
        to_raw = today.isoformat()
    elif mode == "yesterday":
        yest = today - timedelta(days=1)
        from_raw = to_raw = yest.isoformat()
    elif mode == "last3":
        start = today - timedelta(days=3)
        from_raw = start.isoformat()
        to_raw = today.isoformat()
    elif mode == "custom":
        await query.edit_message_text(
            get_custom_period_help(translator),
            parse_mode="Markdown",
        )
        context.user_data["stmt_waiting_dates"] = True
        return
    else:
        return

    from_ts = unix_from_str(from_raw, is_to=False)
    to_ts = unix_from_str(to_raw, is_to=True)

    await generate_and_send_statement(
        source=query,
        context=context,
        user_row=user_row,
        account_key=account_key,
        from_ts=from_ts,
        to_ts=to_ts,
        from_raw=from_raw,
        to_raw=to_raw,
    )


async def generate_and_send_statement(
    source,
    context: ContextTypes.DEFAULT_TYPE,
    user_row: Dict[str, Any],
    account_key: str,  # "all" или "<id>"
    from_ts: int,
    to_ts: int,
    from_raw: str,
    to_raw: str,
):
    translator = get_translator_for_user(user_row)
    action_params = {
        "from": from_raw,
        "to": to_raw,
        "accounts": [],
    }

    def log_action(result: int, output: str) -> None:
        try:
            log_user_action(
                user_id=user_row["id"],
                action_name="statement",
                result=result,
                params=action_params,
                output=output,
            )
        except Exception:
            logging.exception("Failed to log statement action")

    # --- проверка лимита дней ---
    if not user_has_unlimited_days(user_row):
        days = (to_ts - from_ts) / 86400.0
        if days > user_row["max_days"] + 1e-6:
            await _reply(
                source,
                translator.t("errors.period_limit", {"days": user_row["max_days"]}),
            )
            log_action(0, "Период превышает допустимый лимит")
            return

    ignore_ibans = get_ignore_ibans_norm()

    available_accounts = get_available_accounts_for_user(user_row)
    if account_key == "all":
        accounts = available_accounts
    else:
        try:
            acc_id = int(account_key)
        except ValueError:
            await _reply(source, translator.t("errors.invalid_card"))
            log_action(0, "Некорректный идентификатор карты")
            return
        accounts = [acc for acc in available_accounts if acc["id"] == acc_id]

    if not accounts:
        await _reply(source, translator.t("statement.no_accounts"))
        log_action(0, "Нет доступных карт для выписки")
        return

    from datetime import datetime

    rows: List[Dict[str, Any]] = []

    org_cache: Dict[int, Dict[str, Any]] = {}
    tokens: set[str] = set()
    account_labels: list[str] = []

    for acc in accounts:
        org_id = acc.get("organization_id")
        if org_id is None:
            continue

        org = org_cache.get(org_id)
        if org is None:
            org = get_organization_by_id(org_id)
            org_cache[org_id] = org

        if not org or not org.get("is_active"):
            continue

        token = org.get("token")
        if not token:
            continue

        tokens.add(token)

        org_name = org.get("name") if org else "?"
        account_labels.append(f"{org_name} – {acc['name']}")

    action_params["accounts"] = account_labels

    if not tokens:
        await _reply(
            source,
            translator.t("statement.no_active_tokens"),
        )
        log_action(0, "Нет активных организаций с токенами")
        return

    max_wait_left = max(get_statement_wait_left(context, token) for token in tokens)
    if max_wait_left > 0:
        msg = translator.t("errors.monobank_rate_limit") + "\n"
        msg += translator.t("errors.monobank_retry_in", {"seconds": max_wait_left})
        await _reply(source, msg)
        log_action(0, msg)
        return

    for acc in accounts:
        org_id = acc.get("organization_id")
        org = org_cache.get(org_id)
        if not org or not org.get("is_active"):
            continue

        token = org.get("token")
        if not token:
            continue

        try:
            items = fetch_statement(token, acc["mono_account_id"], from_ts, to_ts)
            mark_statement_call(context, token)
        except HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait_left = get_statement_wait_left(context, token)
                msg = translator.t("errors.monobank_rate_limit") + "\n"
                if wait_left > 0:
                    msg += translator.t(
                        "errors.monobank_retry_in", {"seconds": wait_left}
                    )
                else:
                    msg += translator.t("errors.monobank_retry_later")
                await _reply(source, msg)
                log_action(0, msg)
                return
            raise

        flows_allowed = acc.get("access_permissions") or {"in"}
        allow_in = "in" in flows_allowed
        allow_out = "out" in flows_allowed

        filtered_items, included_flows = filter_income_and_ignore(
            items,
            ignore_ibans,
            allow_in=allow_in,
            allow_out=allow_out,
        )

        if not filtered_items:
            continue

        flow_label = _flows_to_payments_label(included_flows, translator)

        for it in sorted(filtered_items, key=lambda x: int(x.get("time", 0))):
            t = int(it.get("time", 0))
            dt_str = datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S")
            amount = int(it.get("amount", 0)) / 100.0
            flow = "out" if amount < 0 else "in"
            comment = it.get("comment") or it.get("description") or ""

            rows.append(
                {
                    "_token_id": acc["organization_id"],
                    "_account_id": acc["id"],
                    "token_name": org["name"],
                    "account_name": acc["name"],
                    "datetime": dt_str,
                    "amount": amount,
                    "comment": comment,
                    "flow": flow,
                    "account_flow_label": flow_label,
                }
            )

    if not rows:
        msg = translator.t("payments.no_payments_period")
        await _reply(source, msg)
        log_action(0, msg)
        return

    rows.sort(key=lambda r: (r["_token_id"], r["_account_id"], r["datetime"]))

    filename = f"выписка_{from_raw}_{to_raw}.xlsx"
    output_path = os.path.join(os.getcwd(), filename)
    write_xlsx(output_path, rows)

    if hasattr(source, "effective_chat") and source.effective_chat:
        chat_id = source.effective_chat.id
    elif hasattr(source, "message") and source.message:
        chat_id = source.message.chat_id
    else:
        logging.warning("Cannot determine chat_id for sending statement file")
        log_action(0, "Не удалось определить chat_id для отправки файла")
        return

    await context.bot.send_document(
        chat_id=chat_id,
        document=open(output_path, "rb"),
        filename=filename,
        caption=translator.t(
            "statement.file_caption", {"from": from_raw, "to": to_raw}
        ),
    )

    log_action(1, filename)


# --- Админ-меню (entry point) ---


async def handle_admin_menu(
    update: Update, context: ContextTypes.DEFAULT_TYPE, user_row: Dict[str, Any]
):
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "➕ Добавить организацию", callback_data="admin:add_org"
                ),
            ],
            [
                InlineKeyboardButton("🏦 Счета", callback_data="admin:accounts"),
            ],
            [
                InlineKeyboardButton("👥 Пользователи", callback_data="admin:users"),
            ],
        ]
    )

    if update.message:
        await update.message.reply_text(
            "🛠 Меню администратора:",
            reply_markup=keyboard,
        )
    elif update.callback_query:
        await update.callback_query.edit_message_text(
            "🛠 Меню администратора:",
            reply_markup=keyboard,
        )


# --- Общий текстовый хендлер ---


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return

    text = (update.message.text or "").strip()
    logging.info("📩 TEXT: '%s', user_data=%s", text, dict(context.user_data))

    user_row = await ensure_active_user(update, context)
    if not user_row:
        return

    translator = get_translator_for_user(user_row)

    admin_mode = context.user_data.get("admin_mode")
    if admin_mode and user_row["role"] == "admin":
        if admin_mode == "approve_set_friendly_name":
            pending = context.user_data.get("pending_user_setup") or {}
            if not pending:
                context.user_data.pop("admin_mode", None)
                await update.message.reply_text("Данные пользователя утеряны. Попробуйте снова.")
                return
            friendly = text.strip()
            if not friendly:
                await update.message.reply_text("Friendly name не может быть пустым. Введите значение ещё раз.")
                return
            pending["friendly_name"] = friendly
            context.user_data["pending_user_setup"] = pending
            context.user_data["admin_mode"] = "approve_set_max_days"
            suggested = pending.get("suggested_max_days", 0)
            await update.message.reply_text(
                "Введите `max_days` (целое число, 0 = без ограничений)\n"
                f"Рекомендация для роли {pending['role']}: {suggested}",
                parse_mode="Markdown",
            )
            return

        if admin_mode == "approve_set_max_days":
            pending = context.user_data.get("pending_user_setup") or {}
            if not pending or "friendly_name" not in pending:
                context.user_data.pop("admin_mode", None)
                await update.message.reply_text("Данные пользователя утеряны. Попробуйте снова.")
                return
            try:
                max_days = int(text.strip())
                if max_days < 0:
                    raise ValueError
            except ValueError:
                await update.message.reply_text(
                    "max_days должно быть целым числом ≥ 0. Попробуйте ещё раз."
                )
                return

            target_id = pending["target_id"]
            role = pending["role"]
            friendly = pending["friendly_name"]

            update_user_role(target_id, role, max_days=max_days)
            update_user_friendly_name(target_id, friendly)

            context.user_data.pop("admin_mode", None)
            context.user_data.pop("pending_user_setup", None)

            await update.message.reply_text(
                f"✅ Пользователь {target_id} получил роль `{role}`.\n"
                f"Friendly name: {friendly}\n"
                f"max_days: {max_days}",
                parse_mode="Markdown",
            )

            try:
                from telegram import ReplyKeyboardRemove

                if role == "blocked":
                    txt = "⛔ Вам отказано в доступе к боту. Обратитесь к администратору."
                    await context.bot.send_message(
                        chat_id=target_id,
                        text=txt,
                        reply_markup=ReplyKeyboardRemove(),
                    )
                elif role in ("manager", "accountant", "admin"):
                    txt = "✅ Вам предоставлен доступ к боту."
                    await context.bot.send_message(
                        chat_id=target_id,
                        text=txt,
                        reply_markup=build_main_menu(role),
                    )
                else:
                    txt = f"Ваша роль в боте изменена на: {role}."
                    await context.bot.send_message(chat_id=target_id, text=txt)
            except Exception:
                pass

            await handle_admin_menu(update, context, user_row)
            return

        if admin_mode == "edit_user_friendly_name":
            target_id = context.user_data.get("edit_user_target_id")
            if not target_id:
                context.user_data.pop("admin_mode", None)
                await update.message.reply_text("Нет выбранного пользователя.")
                return
            friendly = text.strip()
            if not friendly:
                await update.message.reply_text("Имя не может быть пустым. Введите значение снова.")
                return
            update_user_friendly_name(target_id, friendly)
            context.user_data.pop("admin_mode", None)
            context.user_data.pop("edit_user_target_id", None)
            await update.message.reply_text("Friendly name обновлено.")
            return

        if admin_mode == "edit_user_max_days":
            target_id = context.user_data.get("edit_user_target_id")
            if not target_id:
                context.user_data.pop("admin_mode", None)
                await update.message.reply_text("Нет выбранного пользователя.")
                return
            try:
                max_days = int(text.strip())
                if max_days < 0:
                    raise ValueError
            except ValueError:
                await update.message.reply_text("Введите целое число ≥ 0.")
                return
            user_info = get_user(target_id)
            if not user_info:
                context.user_data.pop("admin_mode", None)
                context.user_data.pop("edit_user_target_id", None)
                await update.message.reply_text("Пользователь не найден.")
                return
            update_user_role(target_id, user_info["role"], max_days=max_days)
            context.user_data.pop("admin_mode", None)
            context.user_data.pop("edit_user_target_id", None)
            await update.message.reply_text("max_days обновлён.")
            return

        # --- добавление организации ---
        if admin_mode == "add_org_name":
            context.user_data["new_org_name"] = text
            context.user_data["admin_mode"] = "add_org_token"

            await update.message.reply_text(
                "Теперь отправьте *токен Monobank* для этой организации:",
                parse_mode="Markdown",
            )
            return

        if admin_mode == "add_org_token":
            org_name = (context.user_data.get("new_org_name") or "").strip()
            token = text.strip()

            if not org_name or not token:
                context.user_data.pop("admin_mode", None)
                context.user_data.pop("new_org_name", None)
                await update.message.reply_text(
                    "Имя организации или токен пустые. Попробуйте ещё раз через меню Администрирования."
                )
                return

            org = insert_organization(org_name, token)

            context.user_data.pop("admin_mode", None)
            context.user_data.pop("new_org_name", None)

            await update.message.reply_text(
                f"✅ Организация добавлена.\n\n"
                f"ID: {org['id']}\n"
                f"Имя: {org['name']}",
            )

            await handle_admin_menu(update, context, user_row)
            return

        if admin_mode == "add_account_name":
            acc_name = text.strip()
            if not acc_name:
                await update.message.reply_text(
                    "Имя счёта не может быть пустым. Введите другое значение:"
                )
                return

            org_id = context.user_data.get("acc_org_id")
            mono_id = context.user_data.get("acc_mono_id")
            acc_iban = context.user_data.get("acc_iban")
            currency_code = context.user_data.get("acc_currency_code")

            if not org_id or not mono_id:
                context.user_data.pop("admin_mode", None)
                await update.message.reply_text(
                    "Данные о счёте потеряны. Начните добавление заново через меню Администрирования."
                )
                return

            currency_value = None
            if currency_code not in (None, ""):
                try:
                    currency_value = int(currency_code)
                except (ValueError, TypeError):
                    currency_value = None

            acc = insert_account(
                organization_id=int(org_id),
                mono_account_id=mono_id,
                name=acc_name,
                iban=acc_iban,
                currency_code=currency_value,
            )

            context.user_data.pop("admin_mode", None)
            context.user_data.pop("acc_org_id", None)
            context.user_data.pop("acc_mono_id", None)
            context.user_data.pop("acc_iban", None)
            context.user_data.pop("acc_currency_code", None)
            context.user_data.pop("acc_add_state", None)
            context.user_data.pop("acc_add_state_option", None)
            context.user_data.pop("acc_add_state_org_name", None)

            org = get_organization_by_id(acc["organization_id"])
            org_name = org["name"] if org else "(неизвестно)"

            await update.message.reply_text(
                f"✅ Счёт добавлен.\n\n"
                f"Организация: {org_name}\n"
                f"Счёт: {acc['name']}\n"
                f"Monobank account id: `{acc['mono_account_id']}`\n"
                f"IBAN: `{acc['iban'] or ''}`\n"
                f"Код валюты: `{acc['currency_code'] or ''}`",
                parse_mode="Markdown",
            )

            await handle_admin_menu(update, context, user_row)
            return

    # --- Быстрый ввод периода в меню "Платежи" ---
    pending_pay_acc = context.user_data.get("pay_period_pending")
    if pending_pay_acc is not None and any(ch.isdigit() for ch in text):
        try:
            from_raw, to_raw = parse_custom_period_input(text)
        except ValueError:
            await update.message.reply_text(
                CUSTOM_PERIOD_HELP,
                parse_mode="Markdown",
            )
            return
        context.user_data.pop("pay_period_pending", None)
        from_ts = unix_from_str(from_raw, is_to=False)
        to_ts = unix_from_str(to_raw, is_to=True)
        await show_payments_for_period(
            update, context, user_row, pending_pay_acc, from_ts, to_ts
        )
        return

    # --- Быстрый ввод периода в меню "Выписка" ---
    pending_stmt_key = context.user_data.get("stmt_period_pending")
    if pending_stmt_key is not None and any(ch.isdigit() for ch in text):
        try:
            from_raw, to_raw = parse_custom_period_input(text)
        except ValueError:
            await update.message.reply_text(
                CUSTOM_PERIOD_HELP,
                parse_mode="Markdown",
            )
            return
        context.user_data.pop("stmt_period_pending", None)
        context.user_data["stmt_account_key"] = pending_stmt_key
        from_ts = unix_from_str(from_raw, is_to=False)
        to_ts = unix_from_str(to_raw, is_to=True)
        await generate_and_send_statement(
            source=update,
            context=context,
            user_row=user_row,
            account_key=pending_stmt_key,
            from_ts=from_ts,
            to_ts=to_ts,
            from_raw=from_raw,
            to_raw=to_raw,
        )
        return

    # --- Кастомные даты для Платежей ---
    if "pay_custom_acc_id" in context.user_data:
        try:
            from_raw, to_raw = parse_custom_period_input(text)
        except ValueError:
            await update.message.reply_text(
                CUSTOM_PERIOD_HELP,
                parse_mode="Markdown",
            )
            return
        from_ts = unix_from_str(from_raw, is_to=False)
        to_ts = unix_from_str(to_raw, is_to=True)

        acc_id = context.user_data.pop("pay_custom_acc_id")
        await show_payments_for_period(update, context, user_row, acc_id, from_ts, to_ts)
        return

    # --- Кастомные даты для Выписки ---
    if context.user_data.get("stmt_waiting_dates"):
        account_key = context.user_data.get("stmt_account_key")

        if account_key is None:
            context.user_data["stmt_waiting_dates"] = False
            await update.message.reply_text("Сначала выберите карту для выписки.")
            return

        try:
            from_raw, to_raw = parse_custom_period_input(text)
        except ValueError:
            await update.message.reply_text(
                CUSTOM_PERIOD_HELP,
                parse_mode="Markdown",
            )
            return
        from_ts = unix_from_str(from_raw, is_to=False)
        to_ts = unix_from_str(to_raw, is_to=True)
        context.user_data["stmt_waiting_dates"] = False

        await generate_and_send_statement(
            source=update,
            context=context,
            user_row=user_row,
            account_key=account_key,
            from_ts=from_ts,
            to_ts=to_ts,
            from_raw=from_raw,
            to_raw=to_raw,
        )
        return

    # --- Обычное меню ---
    payments_label = translator.t("main.payments")
    statement_label = translator.t("main.statement")
    balance_label = translator.t("main.balance")
    admin_label = translator.t("main.admin")

    if text == payments_label:
        await handle_payments_entry(update, context, user_row)
    elif text == statement_label:
        await handle_statement_entry(update, context, user_row)
    elif text == balance_label:
        await handle_balance_entry(update, context, user_row)
    elif text == admin_label and user_row["role"] == "admin":
        await handle_admin_menu(update, context, user_row)
    else:
        await update.message.reply_text(
            translator.t("errors.unknown_command"),
            reply_markup=build_main_menu(user_row["role"], translator),
        )


# --- main() ---


def main():
    logging.info("Starting bot.py ...")
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(
        CallbackQueryHandler(approve_callback_handler, pattern=r"^approve:")
    )

    # Платежи
    app.add_handler(CallbackQueryHandler(pay_acc_callback, pattern=r"^pay_acc:"))
    app.add_handler(CallbackQueryHandler(pay_period_callback, pattern=r"^pay_per:"))

    # Выписка
    app.add_handler(CallbackQueryHandler(stmt_acc_callback, pattern=r"^stmt_acc:"))
    app.add_handler(CallbackQueryHandler(stmt_period_callback, pattern=r"^stmt_per:"))

    # Админ-меню
    app.add_handler(CallbackQueryHandler(admin_callback_handler, pattern=r"^admin:"))

    # Управление счетами пользователя
    app.add_handler(
        CallbackQueryHandler(
            admin_user_accounts_menu,
            pattern=rf"^{ADMIN_USER_ACCOUNTS_PREFIX}:\d+$",
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            admin_user_accounts_add,
            pattern=rf"^{ADMIN_USER_ACCOUNTS_ADD_PREFIX}:\d+(?::\d+)?$",
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            admin_user_accounts_del,
            pattern=rf"^{ADMIN_USER_ACCOUNTS_DEL_PREFIX}:\d+(?::\d+)?$",
        )
    )
    app.add_handler(
        CallbackQueryHandler(
            admin_user_accounts_perm,
            pattern=rf"^{ADMIN_USER_ACCOUNTS_PERM_PREFIX}:\d+(?::\d+)?(?::(?:add|del)(?::(?:in|out|balance))?)?$",
        )
    )

    # Общий текстовый хендлер
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler)
    )

    app.run_polling()


if __name__ == "__main__":
    main()
