import pandas as pd
import re
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List, Iterable, Tuple

from sqlalchemy import (
    Column, Integer, String, DateTime, Numeric, Text, ForeignKey
)
from sqlalchemy.orm import declarative_base, relationship, Session


# ---------------------------------------------------------
# 1. МОДЕЛИ БД
# ---------------------------------------------------------

Base = declarative_base()


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


# ---------------------------------------------------------
# 2. ОБЩИЕ СТРУКТУРЫ
# ---------------------------------------------------------

@dataclass
class ParsedPayment:
    date: datetime
    amount: float
    description: str
    guessed_apartment_number: Optional[int] = None
    apartment_id: Optional[int] = None


# ---------------------------------------------------------
# 3. ПОМОГАЮЩИЕ ФУНКЦИИ
# ---------------------------------------------------------

def normalize_colname(name: str) -> str:
    """
    Удаляет лишние пробелы, переводит в lower, заменяет неразрывные пробелы.
    """
    if not isinstance(name, str):
        return ""
    return name.replace("\xa0", " ").strip().lower()


def find_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """
    Универсальный поиск названий колонок.
    candidate — список возможных вариантов (в lower).
    """
    normalized_candidates = [c.lower() for c in candidates]

    for col in df.columns:
        ncol = normalize_colname(col)
        if ncol in normalized_candidates:
            return col

    return None


# ---------------------------------------------------------
# 4. ЧТЕНИЕ И НОРМАЛИЗАЦИЯ ВЫПИСКИ
# ---------------------------------------------------------

def read_sber_statement_excel(path: str) -> pd.DataFrame:
    """
    Считывает excel-выписку СберБизнес с автоматическим определением колонок.
    """
    df = pd.read_excel(path, header=9, dtype=str)

    print("FACTUAL COLUMNS:", df.columns.tolist())  # Поможет в диагностике

    # Ищем ключевые поля
    col_date = find_column(df, ["дата проводки", "date"])
    col_amount = find_column(df, ["сумма", "amount"])
    col_descr = find_column(df, ["назначение платежа", "описание", "description"])

    print("DETECTED:", col_date, col_amount, col_descr)

    if not all([col_date, col_amount, col_descr]):
        raise ValueError(
            f"Не удалось обнаружить нужные колонки. Найдены: date={col_date}, "
            f"amount={col_amount}, descr={col_descr}"
        )

    # Переименуем
    df = df.rename(columns={
        col_date: "date",
        col_amount: "amount",
        col_descr: "description",
    })

    # Оставляем ключевые поля
    df = df[["date", "amount", "description"]]

    # Удаляем пустые строки
    df = df.dropna(how="all")

    # Даты
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Суммы
    df["amount"] = (
        df["amount"]
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
    )
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # Текст
    df["description"] = df["description"].astype(str).str.strip()

    # Убираем пустые и битые строки
    df = df.dropna(subset=["date", "amount"])

    df = df.reset_index(drop=True)
    return df


# ---------------------------------------------------------
# 5. ЛОГИКА ОПРЕДЕЛЕНИЯ КВАРТИРЫ
# ---------------------------------------------------------

def guess_apartment_number(description: str) -> Optional[int]:
    """
    Умные правила определения номера квартиры по назначению платежа.
    """
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
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except:
                pass

    return None


def parse_statement_to_payments(df: pd.DataFrame) -> List[ParsedPayment]:
    result: List[ParsedPayment] = []

    for _, row in df.iterrows():
        p = ParsedPayment(
            date=row["date"],
            amount=float(row["amount"]),
            description=row["description"],
            guessed_apartment_number=guess_apartment_number(row["description"])
        )
        result.append(p)

    return result


# ---------------------------------------------------------
# 6. СВЯЗКА С БАЗОЙ
# ---------------------------------------------------------

def attach_apartment_ids(
    payments: Iterable[ParsedPayment],
    session: Session
) -> Tuple[List[ParsedPayment], List[ParsedPayment]]:

    apartments = session.query(Apartment).all()
    by_number = {a.number: a.id for a in apartments}

    matched, unmatched = [], []

    for p in payments:
        guessed = p.guessed_apartment_number
        if guessed in by_number:
            p.apartment_id = by_number[guessed]
            matched.append(p)
        else:
            unmatched.append(p)

    return matched, unmatched


# ---------------------------------------------------------
# 7. СОХРАНЕНИЕ ДАННЫХ
# ---------------------------------------------------------

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
            raw_info=f"guessed={p.guessed_apartment_number}"
        ))

    session.commit()


# ---------------------------------------------------------
# 8. ГЛАВНАЯ ФУНКЦИЯ ИМПОРТА
# ---------------------------------------------------------

def import_statement(path: str, session: Session):
    df = read_sber_statement_excel(path)
    parsed = parse_statement_to_payments(df)
    matched, unmatched = attach_apartment_ids(parsed, session)
    return matched, unmatched
