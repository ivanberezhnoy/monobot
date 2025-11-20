# db.py

from typing import List, Dict, Any, Optional
from contextlib import contextmanager
import pymysql
import pymysql.cursors

from config import DB_CONFIG


def normalize_permissions_value(value: Optional[str]) -> str:
    """
    Normalizes user_account permissions to one of:
    - "in"
    - "out"
    - "in,out"  (both, order enforced)
    Falls back to "in" if value doesn't contain valid tokens.
    """
    if not value:
        return "in"

    tokens = {chunk.strip().lower() for chunk in value.split(",") if chunk.strip()}
    valid = []
    for key in ("in", "out"):
        if key in tokens:
            valid.append(key)

    if not valid:
        valid = ["in"]

    if len(valid) == 2:
        return "in,out"
    return valid[0]


@contextmanager
def get_connection():
    """
    Opens a new DB connection using DB_CONFIG and yields it.
    Connection is always closed at the end of context.
    """
    conn = pymysql.connect(
        cursorclass=pymysql.cursors.DictCursor,
        **DB_CONFIG,
    )
    try:
        yield conn
    finally:
        conn.close()


# --- Users ---


def get_user(user_id: int) -> Optional[Dict[str, Any]]:
    """
    Returns user row by Telegram user id or None if not found.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE id=%s", (user_id,))
            return cur.fetchone()


def upsert_user_on_start(user_id: int, full_name: str, username: str) -> Dict[str, Any]:
    """
    If user is new → insert with role='pending'.
    If exists → update full_name/username, keep role as is.
    Returns the current user row.
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
    """
    Returns list of Telegram IDs for all users with role='admin'.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users WHERE role='admin'")
            rows = cur.fetchall()
            return [row["id"] for row in rows]


def is_admin(user_id: int) -> bool:
    """
    Returns True if user has role='admin', otherwise False.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT role FROM users WHERE id=%s", (user_id,))
            row = cur.fetchone()
            if not row:
                return False
            return row["role"] == "admin"


def update_user_role(user_id: int, role: str, max_days: Optional[int] = None) -> None:
    """
    Updates user role and (optionally) max_days.
    If max_days is None, existing max_days is preserved.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            if max_days is None:
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
    Returns all users for admin UI.
    Ordered by role priority and then full_name.
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
    """
    Returns all users with role='pending'.
    (Currently not used directly, but left as a helper.)
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM users WHERE role='pending'")
            return cur.fetchall()


# --- Accounts and access ---


def list_all_active_accounts() -> List[Dict[str, Any]]:
    """
    Returns all active accounts.
    Used for admin/accountant, and also when assigning accounts to users.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM accounts
                WHERE is_active = 1
                ORDER BY name
                """
            )
            return cur.fetchall()


def get_accounts_for_user(user_id: int) -> List[Dict[str, Any]]:
    """
    Returns all active accounts explicitly granted to the user (via user_accounts).
    For admin/accountant, bot uses list_all_active_accounts() instead.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.*, ua.permissions
                FROM accounts a
                JOIN user_accounts ua ON ua.account_id = a.id
                WHERE ua.user_id = %s
                  AND a.is_active = 1
                ORDER BY a.name
                """,
                (user_id,),
            )
            return cur.fetchall()


def get_account_by_id(account_id: int) -> Optional[Dict[str, Any]]:
    """
    Returns single account row by id or None.
    """
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
    Inserts new Monobank account and returns its row.
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
    Returns all accounts of a given organization (active and inactive), ordered by name.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM accounts
                WHERE organization_id = %s
                ORDER BY name
                """,
                (org_id,),
            )
            return cur.fetchall()


def grant_account_to_user(user_id: int, account_id: int) -> None:
    """
    Grants user access to given account (user_accounts table).
    Safe for repeated calls: uses INSERT IGNORE to avoid duplicate errors.

    Expected schema (simplified):

        user_accounts(
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id BIGINT NOT NULL,
            account_id INT NOT NULL,
            UNIQUE KEY uq_user_account (user_id, account_id),
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (account_id) REFERENCES accounts(id)
        )
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            # MySQL-specific: INSERT IGNORE will do nothing if row already exists
            cur.execute(
                """
                INSERT IGNORE INTO user_accounts (user_id, account_id, permissions)
                VALUES (%s, %s, %s)
                """,
                (user_id, account_id, "in"),
            )
        conn.commit()


def revoke_account_from_user(user_id: int, account_id: int) -> None:
    """
    Revokes user's access to given account (removes from user_accounts).
    If there is no such row, DELETE just affects 0 rows (no error).
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM user_accounts
                WHERE user_id = %s AND account_id = %s
                """,
                (user_id, account_id),
            )
        conn.commit()


def get_user_account_permissions_map(user_id: int) -> Dict[int, str]:
    """
    Returns mapping account_id -> permissions string for the given user.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT account_id, permissions
                FROM user_accounts
                WHERE user_id = %s
                """,
                (user_id,),
            )
            rows = cur.fetchall()
            return {
                row["account_id"]: normalize_permissions_value(row.get("permissions"))
                for row in rows
            }


def update_user_account_permissions(user_id: int, account_id: int, permissions: str) -> bool:
    """
    Updates permissions for a particular (user, account) pair.
    Returns True if a row was updated.
    """
    normalized = normalize_permissions_value(permissions)
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE user_accounts
                SET permissions = %s
                WHERE user_id = %s AND account_id = %s
                """,
                (normalized, user_id, account_id),
            )
        conn.commit()
        return cur.rowcount > 0


# --- Organizations ---


def get_organization_by_id(org_id: int) -> Optional[Dict[str, Any]]:
    """
    Returns organization row by id or None.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM organizations WHERE id=%s", (org_id,))
            return cur.fetchone()


def list_organizations() -> List[Dict[str, Any]]:
    """
    Returns list of all active organizations.
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT *
                FROM organizations
                WHERE is_active = 1
                ORDER BY name
                """
            )
            return cur.fetchall()


def insert_organization(name: str, token: str) -> Dict[str, Any]:
    """
    Creates new Monobank organization/token and returns its row.

    Expected schema (simplified):

        organizations(
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(...),
            token VARCHAR(...),
            is_active TINYINT(1) NOT NULL DEFAULT 1
        )
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


# --- Ignore IBANs ---


def get_ignore_ibans_norm() -> set[str]:
    """
    Returns a set of normalized IBANs to ignore for incoming payments.

    Expected schema:

        ignore_counter_iban(
            id INT AUTO_INCREMENT PRIMARY KEY,
            iban_norm VARCHAR(...) NOT NULL
        )
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT iban_norm FROM ignore_counter_iban")
            rows = cur.fetchall()
            return {row["iban_norm"] for row in rows if row["iban_norm"]}
