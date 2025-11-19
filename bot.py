# bot.py

import os
import time
import logging
from typing import List, Dict, Any

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
)
from monobank_api import (
    unix_from_str,
    fetch_statement,
    filter_income_and_ignore,
)
from report_xlsx import write_xlsx

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

STATEMENT_MIN_INTERVAL = 60  # секунды – лимит Monobank на выписку по одному токену


# --- Вспомогательные функции / меню ---


def build_main_menu(role: str) -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton("📥 Платежи"), KeyboardButton("📄 Выписка")],
    ]
    if role == "admin":
        buttons.append([KeyboardButton("🛠 Администрирование")])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)


def user_allowed_for_menu(user_row: Dict[str, Any]) -> bool:
    return user_row["role"] in ("manager", "accountant", "admin")


def user_has_unlimited_days(user_row: Dict[str, Any]) -> bool:
    if user_row["role"] in ("admin", "accountant"):
        return True
    return user_row["max_days"] <= 0


def get_available_accounts_for_user(user_row: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Для admin/accountant: возвращаем все активные счета.
    Для остальных: только те, что есть в user_accounts.
    """
    if user_row["role"] in ("admin", "accountant"):
        return list_all_active_accounts()
    return get_accounts_for_user(user_row["id"])


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

    # если совсем непонятно, можно ничего не делать или залогировать
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

    if row["role"] == "admin":
        await update.message.reply_text(
            "Привет, администратор 👋",
            reply_markup=build_main_menu("admin"),
        )
        return

    if row["role"] in ("manager", "accountant"):
        await update.message.reply_text(
            "Добро пожаловать! Вы авторизованы ✅",
            reply_markup=build_main_menu(row["role"]),
        )
        return

    if row["role"] == "blocked":
        await update.message.reply_text(
            "⛔ Доступ к боту запрещён. Обратитесь к администратору."
        )
        return

    # pending
    await update.message.reply_text(
        "Вы отправили запрос на доступ. Ожидайте подтверждения администратора."
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


async def admin_user_accounts_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data  # формат "admin_user_accounts:<user_id>"
    _, user_id_str = data.split(":", 1)
    user_id = int(user_id_str)

    user = get_user(user_id)
    if not user:
        await query.edit_message_text("Пользователь не найден.")
        return

    user_accounts = get_accounts_for_user(user_id)  # счета, доступные этому юзеру
    all_accounts = list_all_active_accounts()  # все активные счета

    user_acc_ids = {acc["id"] for acc in user_accounts}

    lines: list[str] = [f"Пользователь: {user['full_name']}", "", "Доступные счета:"]

    if not user_accounts:
        lines.append("  — нет ни одного счета")
    else:
        for acc in user_accounts:
            org = get_organization_by_id(acc["organization_id"])
            org_name = org["name"] if org else "?"
            lines.append(f"  • {org_name} – {acc['name']}")

    text = "\n".join(lines)

    keyboard = [
        [
            InlineKeyboardButton(
                "➕ Добавить счёт",
                callback_data=f"{ADMIN_USER_ACCOUNTS_ADD_PREFIX}:{user_id}",
            ),
        ],
        [
            InlineKeyboardButton(
                "➖ Удалить счёт",
                callback_data=f"{ADMIN_USER_ACCOUNTS_DEL_PREFIX}:{user_id}",
            ),
        ],
        [
            InlineKeyboardButton("⬅️ Назад", callback_data=f"admin:user:{user_id}"),
        ],
    ]

    await query.edit_message_text(
        text=text,
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def admin_user_accounts_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

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
                "У пользователя уже есть доступ ко всем счетам.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "⬅️ Назад",
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
            label = f"{org_name} – {acc['name']}"
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
                    "⬅️ Назад",
                    callback_data=f"{ADMIN_USER_ACCOUNTS_PREFIX}:{user_id}",
                )
            ]
        )

        await query.edit_message_text(
            text="Выберите счёт, который нужно добавить пользователю:",
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
                        "⬅️ Назад",
                        callback_data=f"{ADMIN_USER_ACCOUNTS_PREFIX}:{user_id}",
                    )
                ]
            ]
        )
        await query.edit_message_text("Счёт добавлен пользователю.", reply_markup=keyboard)


async def admin_user_accounts_del(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data  # "admin_user_accounts_del:<user_id>" или "...:<user_id>:<account_id>"
    parts = data.split(":")
    if len(parts) == 2:
        # шаг 1: показать список счетов для удаления
        _, user_id_str = parts
        user_id = int(user_id_str)

        user_accounts = get_accounts_for_user(user_id)

        if not user_accounts:
            await query.edit_message_text(
                "У пользователя нет счетов для удаления.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "⬅️ Назад",
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
            label = f"{org_name} – {acc['name']}"
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
                    "⬅️ Назад",
                    callback_data=f"{ADMIN_USER_ACCOUNTS_PREFIX}:{user_id}",
                )
            ]
        )

        await query.edit_message_text(
            text="Выберите счёт, который нужно удалить у пользователя:",
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
                        "⬅️ Назад",
                        callback_data=f"{ADMIN_USER_ACCOUNTS_PREFIX}:{user_id}",
                    )
                ]
            ]
        )
        await query.edit_message_text("Счёт удалён у пользователя.", reply_markup=keyboard)


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

    # Определяем max_days по роли
    if role == "manager":
        max_days = 7
    elif role in ("accountant", "admin"):
        max_days = 0
    elif role == "pending":
        max_days = 3
    else:  # blocked и прочие
        max_days = 0

    update_user_role(uid, role, max_days=max_days)

    u = get_user(uid)
    uname = ""
    if u and u.get("username"):
        uname = f"@{u['username']}"

    await query.edit_message_text(
        f"✅ Роль пользователя {uid} {uname} установлена: `{role}`",
        parse_mode="Markdown",
    )

    # Пытаемся уведомить самого пользователя
    try:
        from telegram import ReplyKeyboardRemove

        if role == "blocked":
            txt = "⛔ Вам отказано в доступе к боту. Обратитесь к администратору."
            await context.bot.send_message(
                chat_id=uid,
                text=txt,
                reply_markup=ReplyKeyboardRemove(),
            )
        elif role in ("manager", "accountant", "admin"):
            txt = "✅ Вам предоставлен доступ к боту."
            await context.bot.send_message(
                chat_id=uid,
                text=txt,
                reply_markup=build_main_menu(role),
            )
        elif role == "pending":
            txt = "Ваш статус в боте: pending. Ожидайте решения администратора."
            await context.bot.send_message(
                chat_id=uid,
                text=txt,
            )
        else:
            txt = f"Ваша роль в боте изменена на: {role}."
            await context.bot.send_message(
                chat_id=uid,
                text=txt,
            )

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

            name = u["full_name"] or ""
            uname = f"@{u['username']}" if u["username"] else ""
            label = f"{role_icon} {u['id']} – {name} {uname}".strip()

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
    if action in ("acc_org", "acc_add", "acc_list", "acc_info", "user"):
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

        context.user_data["admin_mode"] = "add_account_mono_id"
        context.user_data["acc_org_id"] = org["id"]
        context.user_data.pop("acc_mono_id", None)
        context.user_data.pop("acc_name", None)
        context.user_data.pop("acc_iban", None)
        context.user_data.pop("acc_currency_code", None)

        await query.edit_message_text(
            f"Организация: *{org['name']}*\n\n"
            "Введите *идентификатор счёта* в Monobank (account id из client-info):",
            parse_mode="Markdown",
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
        text = (
            f"👤 Пользователь: *{u['full_name'] or ''}*\n"
            f"ID: `{u['id']}`\n"
            f"Username: {uname}\n"
            f"Роль: `{role}`\n"
            f"MaxDays: {max_days}\n\n"
            "Выберите новую роль или управляйте доступом к счетам:"
        )

        kb = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "👤 Pending", callback_data=f"admin:userrole:pending:{u['id']}"
                    ),
                    InlineKeyboardButton(
                        "👔 Менеджер", callback_data=f"admin:userrole:manager:{u['id']}"
                    ),
                ],
                [
                    InlineKeyboardButton(
                        "📊 Бухгалтер",
                        callback_data=f"admin:userrole:accountant:{u['id']}",
                    ),
                    InlineKeyboardButton(
                        "👑 Админ", callback_data=f"admin:userrole:admin:{u['id']}"
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
                        "⛔ Blocked", callback_data=f"admin:userrole:blocked:{u['id']}"
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

        if new_role == "manager":
            max_days = 7
        elif new_role in ("accountant", "admin"):
            max_days = 0
        elif new_role == "pending":
            max_days = 3
        else:
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
    if not user_row:
        await update.message.reply_text("Используйте /start для регистрации в боте.")
        return None

    if user_row["role"] in ("pending",):
        await update.message.reply_text(
            "Ваш запрос на доступ ещё не одобрен. Обратитесь к администратору."
        )
        return None

    if user_row["role"] == "blocked":
        await update.message.reply_text(
            "⛔ Доступ к боту запрещён. Обратитесь к администратору."
        )
        return None

    return user_row


# --- Платежи (текстовый вывод) ---


async def handle_payments_entry(
    update: Update, context: ContextTypes.DEFAULT_TYPE, user_row: Dict[str, Any]
):
    accounts = get_available_accounts_for_user(user_row)

    if not accounts:
        await update.message.reply_text(
            "К сожалению, у вас нет доступных карт. Обратитесь к администратору."
        )
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
                "💳 Все карты",
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
        "Выберите карту (или все карты):",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def ask_period_for_payments(
    source, context: ContextTypes.DEFAULT_TYPE, user_row: Dict[str, Any], account_key: str
):
    """
    account_key: "all" или строковый id карты.
    """
    if account_key == "all":
        card_label = "Все доступные карты"
    else:
        acc = get_account_by_id(int(account_key))
        if not acc:
            await _reply(source, "Карта не найдена.")
            return
        org = get_organization_by_id(acc["organization_id"])
        org_name = org["name"] if org else "?"
        card_label = f"{org_name} – {acc['name']}"

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "⏱ Последний час",
                    callback_data=f"pay_per:{account_key}:last_hour",
                ),
            ],
            [
                InlineKeyboardButton(
                    "📅 Сегодня", callback_data=f"pay_per:{account_key}:today"
                ),
                InlineKeyboardButton(
                    "📅 Вчера", callback_data=f"pay_per:{account_key}:yesterday"
                ),
            ],
            [
                InlineKeyboardButton(
                    "✏️ Выбрать период",
                    callback_data=f"pay_per:{account_key}:custom",
                ),
            ],
        ]
    )

    text = f"Карта: *{card_label}*\nВыберите период:"
    if hasattr(source, "message") and source.message:
        await source.message.reply_text(
            text, reply_markup=keyboard, parse_mode="Markdown"
        )
    else:
        await source.edit_message_text(
            text, reply_markup=keyboard, parse_mode="Markdown"
        )


async def pay_acc_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_row = get_user(query.from_user.id)
    if not user_row or not user_allowed_for_menu(user_row):
        await query.edit_message_text("Нет доступа.")
        return

    _, acc_key = query.data.split(":")  # "all" или "<id>"

    await ask_period_for_payments(query, context, user_row, acc_key)


async def pay_period_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_row = get_user(query.from_user.id)
    if not user_row or not user_allowed_for_menu(user_row):
        await query.edit_message_text("Нет доступа.")
        return

    # data: "pay_per:<account_key>:<mode>"
    _, acc_key, mode = query.data.split(":")

    from datetime import datetime, timedelta

    now = datetime.now()
    today = now.date()

    if mode == "last_hour":
        from_ts = int((now - timedelta(hours=1)).timestamp())
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
            "Отправьте период в формате:\n"
            "`YYYY-MM-DD YYYY-MM-DD`\n"
            "Например: `2025-11-04 2025-11-05`",
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

    # --- Проверка лимита по дням ---
    if not user_has_unlimited_days(user_row):
        days = (to_ts - from_ts) / 86400.0
        if days > user_row["max_days"] + 1e-6:
            await _reply(
                source,
                f"Выбранный период превышает допустимый лимит {user_row['max_days']} дней.",
            )
            return

    ignore_ibans = get_ignore_ibans_norm()

    # --- Список карт ---
    if account_key == "all":
        accounts = get_available_accounts_for_user(user_row)
    else:
        acc = get_account_by_id(int(account_key))
        if not acc:
            await _reply(source, "Карта не найдена.")
            return
        accounts = [acc]

    if not accounts:
        await _reply(source, "Нет доступных карт.")
        return

    from datetime import datetime

    all_lines: list[str] = []
    total_ops = 0

    # --- Кеш организаций и сбор токенов ---
    org_cache: Dict[int, Dict[str, Any]] = {}
    tokens: set[str] = set()

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

    if not tokens:
        await _reply(
            source,
            "Для выбранных карт не найдено активных организаций с токенами Monobank.",
        )
        return

    # --- Проверяем лимит Monobank по всем токенам ---
    max_wait_left = max(get_statement_wait_left(context, token) for token in tokens)
    if max_wait_left > 0:
        await _reply(
            source,
            "Monobank дозволяє отримувати виписку не частіше, ніж раз на хвилину.\n"
            f"Спробуйте ще раз через {max_wait_left} с.",
        )
        return

    # --- Основной цикл по аккаунтам ---
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

        try:
            items = fetch_statement(token, acc["mono_account_id"], from_ts, to_ts)
            mark_statement_call(context, token)
        except HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait_left = get_statement_wait_left(context, token)
                msg = "Monobank просить не робити виписку частіше, ніж раз на хвилину.\n"
                if wait_left > 0:
                    msg += f"Спробуйте ще раз через приблизно {wait_left} с."
                else:
                    msg += "Спробуйте ще раз трохи пізніше."
                await _reply(source, msg)
                return
            raise

        items = filter_income_and_ignore(items, ignore_ibans)

        if not items:
            continue

        if account_key == "all":
            all_lines.append(f"💳 {card_label} — приходные операции:")

        for it in sorted(items, key=lambda x: int(x.get("time", 0))):
            t = int(it.get("time", 0))
            dt_str = datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S")
            amount = int(it.get("amount", 0)) / 100.0
            comment = it.get("comment") or it.get("description") or ""
            line = f"{dt_str} — {amount:.2f} UAH"
            all_lines.append(line)
            if comment:
                all_lines.append(f"  {comment}")
            total_ops += 1

        if account_key == "all":
            all_lines.append("")

    if total_ops == 0:
        await _reply(source, "Нет приходных операций за выбранный период.")
        return

    text = "\n".join(all_lines)
    await _reply(source, text)


# --- Выписка (Excel) ---


async def ask_statement_period(
    source, context: ContextTypes.DEFAULT_TYPE, account: Dict[str, Any] | None
):
    """
    account:
      - None  → режим "Все карты"
      - dict  → конкретная карта
    """
    if account is None:
        label = "Все доступные карты"
    else:
        org = get_organization_by_id(account["organization_id"])
        org_name = org["name"] if org else "?"
        label = f"{org_name} – {account['name']}"

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📅 Сегодня", callback_data="stmt_per:today"),
                InlineKeyboardButton("📅 Вчера", callback_data="stmt_per:yesterday"),
            ],
            [
                InlineKeyboardButton(
                    "📅 Прошлые 3 дня", callback_data="stmt_per:last3"
                ),
            ],
            [
                InlineKeyboardButton(
                    "✏️ Выбрать период", callback_data="stmt_per:custom"
                ),
            ],
        ]
    )

    text = f"Карта: *{label}*\nВыберите период:"
    if hasattr(source, "message") and source.message:
        await source.message.reply_text(
            text, reply_markup=keyboard, parse_mode="Markdown"
        )
    else:
        await source.edit_message_text(
            text, reply_markup=keyboard, parse_mode="Markdown"
        )


async def handle_statement_entry(
    update: Update, context: ContextTypes.DEFAULT_TYPE, user_row: Dict[str, Any]
):
    accounts = get_available_accounts_for_user(user_row)

    if not accounts:
        await update.message.reply_text(
            "К сожалению, у вас нет доступных карт для выписки. Обратитесь к администратору."
        )
        return

    if len(accounts) == 1:
        acc = accounts[0]
        context.user_data["stmt_account_key"] = str(acc["id"])
        await ask_statement_period(update, context, acc)
        return

    keyboard = []

    keyboard.append(
        [InlineKeyboardButton("💳 Все карты", callback_data="stmt_acc:all")]
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
        "Выберите карту для выписки (или все карты):",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def stmt_acc_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_row = get_user(query.from_user.id)
    if not user_row or not user_allowed_for_menu(user_row):
        await query.edit_message_text("Нет доступа.")
        return

    _, acc_key = query.data.split(":")  # "all" или "<id>"

    context.user_data["stmt_account_key"] = acc_key

    if acc_key == "all":
        account = None
    else:
        account = get_account_by_id(int(acc_key))
        if not account:
            await query.edit_message_text("Карта не найдена.")
            return

    await ask_statement_period(query, context, account)


async def stmt_period_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_row = get_user(query.from_user.id)
    if not user_row or not user_allowed_for_menu(user_row):
        await query.edit_message_text("Нет доступа.")
        return

    account_key = context.user_data.get("stmt_account_key")
    if account_key is None:
        await query.edit_message_text("Сначала выберите карту для выписки.")
        return

    _, mode = query.data.split(":")

    from datetime import datetime, timedelta

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
            "Отправьте период в формате:\n"
            "`YYYY-MM-DD YYYY-MM-DD`\n"
            "Например: `2025-11-04 2025-11-05`",
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
    # --- проверка лимита дней ---
    if not user_has_unlimited_days(user_row):
        days = (to_ts - from_ts) / 86400.0
        if days > user_row["max_days"] + 1e-6:
            await _reply(
                source,
                f"Выбранный период превышает допустимый лимит {user_row['max_days']} дней.",
            )
            return

    ignore_ibans = get_ignore_ibans_norm()

    # --- формируем список карт ---
    if account_key == "all":
        accounts = get_available_accounts_for_user(user_row)
    else:
        acc = get_account_by_id(int(account_key))
        if not acc:
            await _reply(source, "Карта не найдена.")
            return
        accounts = [acc]

    if not accounts:
        await _reply(source, "Нет доступных карт для выписки.")
        return

    from datetime import datetime

    rows: List[Dict[str, Any]] = []

    org_cache: Dict[int, Dict[str, Any]] = {}
    tokens: set[str] = set()

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

    if not tokens:
        await _reply(
            source,
            "Для выбранных карт не найдено активных организаций с токенами Monobank.",
        )
        return

    max_wait_left = max(get_statement_wait_left(context, token) for token in tokens)
    if max_wait_left > 0:
        await _reply(
            source,
            "Monobank дозволяє отримувати виписку не частіше, ніж раз на хвилину.\n"
            f"Спробуйте ще раз через {max_wait_left} с.",
        )
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
                msg = "Monobank просить не робити виписку частіше, ніж раз на хвилину.\n"
                if wait_left > 0:
                    msg += f"Спробуйте ще раз через приблизно {wait_left} с."
                else:
                    msg += "Спробуйте ще раз трохи пізніше."
                await _reply(source, msg)
                return
            raise

        items = filter_income_and_ignore(items, ignore_ibans)

        for it in sorted(items, key=lambda x: int(x.get("time", 0))):
            t = int(it.get("time", 0))
            dt_str = datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S")
            amount = int(it.get("amount", 0)) / 100.0
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
                }
            )

    if not rows:
        await _reply(source, "Нет приходных операций за выбранный период.")
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
        # fallback
        chat_id = None

    if chat_id is None:
        logging.warning("Cannot determine chat_id for sending statement file")
        return

    await context.bot.send_document(
        chat_id=chat_id,
        document=open(output_path, "rb"),
        filename=filename,
        caption=f"Выписка за период {from_raw} — {to_raw}",
    )


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

    admin_mode = context.user_data.get("admin_mode")
    if admin_mode and user_row["role"] == "admin":
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

        # --- добавление счёта ---
        if admin_mode == "add_account_mono_id":
            context.user_data["acc_mono_id"] = text.strip()
            context.user_data["admin_mode"] = "add_account_name"

            await update.message.reply_text(
                "Введите *имя счёта* (как оно будет отображаться в отчётах):",
                parse_mode="Markdown",
            )
            return

        if admin_mode == "add_account_name":
            context.user_data["acc_name"] = text.strip()
            context.user_data["admin_mode"] = "add_account_iban"

            await update.message.reply_text(
                "Введите *IBAN* (или `-`, если IBAN отсутствует):",
                parse_mode="Markdown",
            )
            return

        if admin_mode == "add_account_iban":
            iban = text.strip()
            if iban == "-":
                iban = None
            context.user_data["acc_iban"] = iban
            context.user_data["admin_mode"] = "add_account_currency"

            await update.message.reply_text(
                "Введите *код валюты* (например, `980` для UAH):",
                parse_mode="Markdown",
            )
            return

        if admin_mode == "add_account_currency":
            try:
                currency_code = int(text.strip())
            except ValueError:
                await update.message.reply_text(
                    "Код валюты должен быть числом (например, `980`). Попробуйте ещё раз."
                )
                return

            org_id = context.user_data.get("acc_org_id")
            mono_id = context.user_data.get("acc_mono_id")
            acc_name = context.user_data.get("acc_name")
            acc_iban = context.user_data.get("acc_iban")

            if not org_id or not mono_id or not acc_name:
                context.user_data.pop("admin_mode", None)
                context.user_data.pop("acc_org_id", None)
                context.user_data.pop("acc_mono_id", None)
                context.user_data.pop("acc_name", None)
                context.user_data.pop("acc_iban", None)
                await update.message.reply_text(
                    "Данные счёта потеряны, попробуйте ещё раз через меню Администрирования."
                )
                return

            acc = insert_account(
                organization_id=int(org_id),
                mono_account_id=mono_id,
                name=acc_name,
                iban=acc_iban,
                currency_code=currency_code,
            )

            context.user_data.pop("admin_mode", None)
            context.user_data.pop("acc_org_id", None)
            context.user_data.pop("acc_mono_id", None)
            context.user_data.pop("acc_name", None)
            context.user_data.pop("acc_iban", None)

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

    # --- Кастомные даты для Платежей ---
    if "pay_custom_acc_id" in context.user_data:
        parts = text.split()
        if len(parts) != 2:
            await update.message.reply_text(
                "Нужно две даты через пробел. Пример:\n"
                "`2025-11-04 2025-11-05`",
                parse_mode="Markdown",
            )
            return

        from_raw, to_raw = parts
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

        parts = text.split()
        if len(parts) != 2:
            await update.message.reply_text(
                "Нужно две даты через пробел. Пример:\n"
                "`2025-11-04 2025-11-05`",
                parse_mode="Markdown",
            )
            return

        from_raw, to_raw = parts
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
    if text == "📥 Платежи":
        await handle_payments_entry(update, context, user_row)
    elif text == "📄 Выписка":
        await handle_statement_entry(update, context, user_row)
    elif text == "🛠 Администрирование" and user_row["role"] == "admin":
        await handle_admin_menu(update, context, user_row)
    else:
        await update.message.reply_text(
            "Выберите действие из меню:",
            reply_markup=build_main_menu(user_row["role"]),
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

    # Общий текстовый хендлер
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler)
    )

    app.run_polling()


if __name__ == "__main__":
    main()
