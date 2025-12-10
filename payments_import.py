import pandas as pd
import re
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List, Iterable, Tuple

from sqlalchemy import (
    Column, Integer, String, DateTime, Numeric, Text, ForeignKey
)
from sqlalchemy.orm import declarative_base, relationship, Session


Base = declarative_base()


# ======================================================
# МОДЕЛИ
# ======================================================

class Apartment(Base):
    __tablename__ = "apartments"
    id = Column(Integer, primary_key=True)
    number = Column(Integer, nullable=False, index=True)
    owner_name = Column(String, nullable=True)


class Payment(Base):
    __tablename__ = "payments"
    id = Column(Integer, primary_key=True)
    apartment_id = Column(Integer, ForeignKey("apartments.id"), nullable=False)
    date = Column(DateTime, nullable=False)
    amount = Column(Numeric(12, 2), nullable=False)
    description = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    apartment = relationship("Apartment")


class UnmatchedPayment(Base):
    __tablename__ = "unmatched_payments"
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, nullable=False)
    amount = Column(Numeric(12, 2), nullable=False)
    description = Column(Text, nullable=False)
    raw_info = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


@dataclass
class ParsedPayment:
    date: datetime
    amount: float
    description: str
    guessed_apartment_number: Optional[int] = None
    apartment_id: Optional[int] = None


# ======================================================
# УТИЛИТЫ
# ======================================================

def normalize(name: str) -> str:
    """Убирает пробелы / неразрывные пробелы / приводит к нижнему регистру."""
    if not isinstance(name, str):
        return ""
    return name.replace("\xa0", " ").strip().lower()


def find_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Находит колонку, имя которой похоже на любое из candidates."""
    for col in df.columns:
        if normalize(col) in candidates:
            return col
    return None


# ======================================================
# ЧТЕНИЕ ВЫПИСКИ
# ======================================================

def read_sber_statement_excel(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, header=9, dtype=str)

    print("FACTUAL COLUMNS:", df.columns.tolist())

    # Ожидаемые имена колонок после нормализации
    date_candidates = ["дата проводки"]
    descr_candidates = ["назначение платежа"]

    amount_candidates_standard = ["сумма"]
    income_candidates = ["сумма по кредиту"]   # поступления
    expense_candidates = ["сумма по дебету"]  # списания

    # Поиск колонок
    col_date = find_column(df, date_candidates)
    col_descr = find_column(df, descr_candidates)

    col_amount = find_column(df, amount_candidates_standard)
    col_credit = find_column(df, income_candidates)
    col_debit = find_column(df, expense_candidates)

    print("DETECTED:", col_date, col_amount, col_credit, col_debit, col_descr)

    if not col_date or not col_descr:
        raise ValueError("Не удалось обнаружить ключевые колонки (Дата / Назначение).")

    # Приоритет суммы:
    # 1. Сумма
    # 2. Сумма по кредиту (поступления)
    if col_amount:
        amount_col = col_amount
    elif col_credit:
        amount_col = col_credit
    else:
        raise ValueError("Не удалось обнаружить ни 'Сумма', ни 'Сумма по кредиту'.")

    # Основные колонки
    df = df[[col_date, amount_col, col_descr]].copy()
    df.columns = ["date", "amount", "description"]

    # Парсим дату
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Парсим суммы
    df["amount"] = (
        df["amount"]
        .astype(str)
        .str.replace(" ", "")
        .str.replace(",", ".", regex=False)
    )
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # Убираем расходные операции (если была "Сумма по дебету")
    if col_debit:
        print("Удаляем расходные операции (Сумма по дебету)…")

    df = df.dropna(subset=["date", "amount"])
    df["description"] = df["description"].astype(str).str.strip()

    print("ПЛАТЕЖИ ПОСЛЕ ОЧИСТКИ:", len(df))

    return df.reset_index(drop=True)


# ======================================================
# ЛОГИКА ОПРЕДЕЛЕНИЯ КВАРТИРЫ
# ======================================================

def guess_apartment_number(description: str) -> Optional[int]:
    if not description:
        return None

    text = description.lower()

    patterns = [
        r"кв\.?\s*(\d+)",
        r"квартира\s+(\d+)",
        r"кв-?(\d+)",
        r"л\/с\s*(\d+)",
    ]

    for pat in patterns:
        m = re.search(pat, text)
        if m:
            try:
                return int(m.group(1))
            except:
                pass

    return None


def parse_statement_to_payments(df: pd.DataFrame) -> List[ParsedPayment]:
    result = []
    for _, row in df.iterrows():
        p = ParsedPayment(
            date=row["date"],
            amount=float(row["amount"]),
            description=row["description"],
            guessed_apartment_number=guess_apartment_number(row["description"])
        )
        result.append(p)
    return result


# ======================================================
# СОПОСТАВЛЕНИЕ С КВАРТИРАМИ
# ======================================================

def attach_apartment_ids(
    payments: Iterable[ParsedPayment],
    session: Session
) -> Tuple[List[ParsedPayment], List[ParsedPayment]]:

    apartments = session.query(Apartment).all()
    number_to_id = {a.number: a.id for a in apartments}

    matched, unmatched = [], []

    for p in payments:
        if p.guessed_apartment_number in number_to_id:
            p.apartment_id = number_to_id[p.guessed_apartment_number]
            matched.append(p)
        else:
            unmatched.append(p)

    return matched, unmatched


# ======================================================
# СОХРАНЕНИЕ
# ======================================================

def save_payments_to_db(session: Session,
                        matched: List[ParsedPayment],
                        unmatched: List[ParsedPayment]):

    for p in matched:
        session.add(Payment(
            apartment_id=p.apartment_id,
            date=p.date,
            amount=p.amount,
            description=p.description
        ))

    for p in unmatched:
        session.add(UnmatchedPayment(
            date=p.date,
            amount=p.amount,
            description=p.description,
            raw_info=str(p.guessed_apartment_number)
        ))

    session.commit()


# ======================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ======================================================

def import_statement(path: str, session: Session):
    df = read_sber_statement_excel(path)
    parsed = parse_statement_to_payments(df)
    matched, unmatched = attach_apartment_ids(parsed, session)
    return matched, unmatched
