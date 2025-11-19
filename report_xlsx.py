# report_xlsx.py

from typing import List, Dict, Any
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, Alignment


def write_xlsx(output_path: str, rows: List[Dict[str, Any]]) -> None:
    """
    rows — список словарей вида:
      {
        "_token_id": int,
        "_account_id": int,
        "token_name": str,
        "account_name": str,
        "datetime": str,
        "amount": float|str,
        "comment": str,
      }

    Структура файла:

    [СМЕРЖЕННЫЙ ЗАГОЛОВОК ТОКЕНА (жирный, 14)]
    [СМЕРЖЕННЫЙ ЗАГОЛОВОК СЧЁТА]
    Дата и время | Сумма | Комментарий
    ... операции ...
    Итого по счёту ...
    ...
    Итого по токену ...
    """

    wb = Workbook()
    ws = wb.active

    DATE_COL = 1
    AMOUNT_COL = 2
    COMMENT_COL = 3
    LAST_COL = 3

    current_row = 1

    current_token_id = None
    current_token_name = ""
    token_total = 0.0

    current_account_id = None
    current_account_name = ""
    account_total = 0.0

    def write_account_total():
        nonlocal current_row, account_total, current_account_name, current_account_id
        if current_account_id is None:
            return
        label = f"Итого по счёту {current_account_name}"
        ws.cell(row=current_row, column=DATE_COL, value=label)
        ws.cell(row=current_row, column=AMOUNT_COL, value=round(account_total, 2))
        ws.cell(row=current_row, column=DATE_COL).font = Font(bold=True)
        ws.cell(row=current_row, column=AMOUNT_COL).font = Font(bold=True)
        current_row += 2  # итог + пустая строка

    def write_token_total():
        nonlocal current_row, token_total, current_token_name, current_token_id
        if current_token_id is None:
            return
        label = f"Итого по токену {current_token_name}"
        ws.cell(row=current_row, column=DATE_COL, value=label)
        ws.cell(row=current_row, column=AMOUNT_COL, value=round(token_total, 2))
        ws.cell(row=current_row, column=DATE_COL).font = Font(bold=True, size=12)
        ws.cell(row=current_row, column=AMOUNT_COL).font = Font(bold=True, size=12)
        current_row += 3  # итог токена + 2 пустых строки

    for row in rows:
        token_id = row["_token_id"]
        account_id = row["_account_id"]
        token_name = row["token_name"]
        account_name = row["account_name"]

        # смена токена
        if current_token_id is not None and token_id != current_token_id:
            write_account_total()
            write_token_total()
            current_account_id = None
            account_total = 0.0
            token_total = 0.0

        if current_token_id != token_id:
            current_token_id = token_id
            current_token_name = token_name
            ws.merge_cells(
                start_row=current_row,
                start_column=DATE_COL,
                end_row=current_row,
                end_column=LAST_COL,
            )
            cell = ws.cell(row=current_row, column=DATE_COL, value=current_token_name)
            cell.font = Font(bold=True, size=14)
            cell.alignment = Alignment(horizontal="center")
            current_row += 2

        # смена счёта внутри токена
        if current_account_id is not None and account_id != current_account_id:
            write_account_total()
            account_total = 0.0

        if current_account_id != account_id:
            current_account_id = account_id
            current_account_name = account_name

            ws.merge_cells(
                start_row=current_row,
                start_column=DATE_COL,
                end_row=current_row,
                end_column=LAST_COL,
            )
            cell = ws.cell(row=current_row, column=DATE_COL, value=current_account_name)
            cell.font = Font(bold=True, size=12)
            cell.alignment = Alignment(horizontal="left")
            current_row += 1

            # заголовок таблицы
            ws.cell(row=current_row, column=DATE_COL, value="Дата и время")
            ws.cell(row=current_row, column=AMOUNT_COL, value="Сумма")
            ws.cell(row=current_row, column=COMMENT_COL, value="Комментарий")
            for col in range(DATE_COL, LAST_COL + 1):
                ws.cell(row=current_row, column=col).font = Font(bold=True)
            current_row += 1

        dt_str = row["datetime"]
        try:
            amt = float(row["amount"])
        except Exception:
            amt = float(str(row["amount"]).replace(",", "."))

        comment = row.get("comment", "")

        ws.cell(row=current_row, column=DATE_COL, value=dt_str)
        ws.cell(row=current_row, column=AMOUNT_COL, value=amt)
        ws.cell(row=current_row, column=COMMENT_COL, value=comment)

        account_total += amt
        token_total += amt

        current_row += 1

    # завершение последнего счёта/токена
    write_account_total()
    write_token_total()

    # автоширина
    for col_idx in range(1, LAST_COL + 1):
        max_len = 0
        col_letter = get_column_letter(col_idx)
        for cell in ws[col_letter]:
            if cell.value is None:
                continue
            length = len(str(cell.value))
            max_len = max(max_len, length)
        ws.column_dimensions[col_letter].width = max_len + 2

    wb.save(output_path)
