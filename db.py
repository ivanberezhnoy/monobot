# db.py

from typing import List, Dict, Any, Optional
from contextlib import contextmanager
import pymysql
import pymysql.cursors

from config import DB_CONFIG


@contextmanager
def get_connection():
    conn = pymysql.connect(
        cursorclass=pymysql.cursors.DictCursor,
        **DB_CONFIG,
    )
    try:
        yield conn
    finally:
        conn.close()


def get_user(user_id: int) -> Optional[Dict[str, Any]]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE id=%s", (user_id,))
            return cur.fetchone()


def upsert_user_on_start(user_id: int, full_name: str, username: str) -> Dict[str, Any]:
    """
    Если юзер новый — вставляем с role='pending'.
    Если уже есть — обновляем имя/username, но роль не трогаем.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE id=%s", (user_id,))
            row = cur.fetchone()
            if row:
                cur.execute(
                    "UPDATE users SET full_name=%s, username=%s WHERE id=%s",
                    (full_name, username, user_id),
                )
                conn.commit()
            else:
                cur.execute(
                    """
                    INSERT INTO users (id, full_name, username, role, max_days)
                    VALUES (%s, %s, %s, 'pending', 3)
                    """,
                    (user_id, full_name, username),
                )
                conn.commit()

            cur.execute("SELECT * FROM users WHERE id=%s", (user_id,))
            return cur.fetchone()

def list_admin_ids() -> List[int]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users WHERE role='admin'")
            rows = cur.fetchall()
            return [row["id"] for row in rows]


def is_admin(user_id: int) -> bool:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT role FROM users WHERE id=%s", (user_id,))
            row = cur.fetchone()
            if not row:
                return False
            return row["role"] == "admin"

def update_user_role(user_id: int, role: str, max_days: Optional[int] = None) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            if max_days is None:
                # оставляем прежний max_days
                cur.execute(
                    "UPDATE users SET role=%s WHERE id=%s",
                    (role, user_id),
                )
            else:
                cur.execute(
                    "UPDATE users SET role=%s, max_days=%s WHERE id=%s",
                    (role, max_days, user_id),
                )
        conn.commit()

def list_users() -> List[Dict[str, Any]]:
    """
    Возвращает список всех пользователей для админа.
    Сортируем по роли и имени.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM users
                ORDER BY
                    CASE role
                        WHEN 'admin' THEN 0
                        WHEN 'accountant' THEN 1
                        WHEN 'manager' THEN 2
                        WHEN 'pending' THEN 3
                        WHEN 'blocked' THEN 4
                        ELSE 5
                    END,
                    full_name
                """
            )
            return cur.fetchall()


def list_pending_users() -> List[Dict[str, Any]]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE role='pending'")
            return cur.fetchall()


# --- Accounts and access ---

def list_all_active_accounts() -> List[Dict[str, Any]]:
    """
    Возвращает все активные счета (для администратора).
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM accounts
                WHERE is_active=1
                ORDER BY name
                """
            )
            return cur.fetchall()
            
def get_accounts_for_user(user_id: int) -> List[Dict[str, Any]]:
    """
    Возвращает список активных счетов, доступных пользователю.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.*
                FROM accounts a
                JOIN user_accounts ua ON ua.account_id = a.id
                WHERE ua.user_id=%s AND a.is_active=1
                ORDER BY a.name
                """,
                (user_id,),
            )
            return cur.fetchall()


def get_account_by_id(account_id: int) -> Optional[Dict[str, Any]]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM accounts WHERE id=%s", (account_id,))
            return cur.fetchone()

def insert_account(
    organization_id: int,
    mono_account_id: str,
    name: str,
    iban: Optional[str],
    currency_code: Optional[int],
) -> Dict[str, Any]:
    """
    Добавляет новый счёт к организации.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO accounts
                    (organization_id, mono_account_id, name, iban, currency_code, is_active)
                VALUES (%s, %s, %s, %s, %s, 1)
                """,
                (organization_id, mono_account_id, name, iban, currency_code),
            )
            acc_id = cur.lastrowid
            conn.commit()

            cur.execute("SELECT * FROM accounts WHERE id=%s", (acc_id,))
            return cur.fetchone()


def list_accounts_by_org(org_id: int) -> List[Dict[str, Any]]:
    """
    Список счетов по организации.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM accounts
                WHERE organization_id=%s
                ORDER BY name
                """,
                (org_id,),
            )
            return cur.fetchall()


def get_organization_by_id(org_id: int) -> Optional[Dict[str, Any]]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM organizations WHERE id=%s", (org_id,))
            return cur.fetchone()

def list_organizations() -> List[Dict[str, Any]]:
    """
    Возвращает список активных организаций.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM organizations WHERE is_active=1 ORDER BY name"
            )
            return cur.fetchall()

def get_ignore_ibans_norm() -> set[str]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT iban_norm FROM ignore_counter_iban")
            rows = cur.fetchall()
            return {row["iban_norm"] for row in rows if row["iban_norm"]}

def insert_organization(name: str, token: str) -> Dict[str, Any]:
    """
    Создаёт новую организацию (Monobank токен) и возвращает её запись.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO organizations (name, token, is_active)
                VALUES (%s, %s, 1)
                """,
                (name, token),
            )
            org_id = cur.lastrowid
            conn.commit()

            cur.execute("SELECT * FROM organizations WHERE id=%s", (org_id,))
            return cur.fetchone()