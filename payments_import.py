import pandas as pd
import re
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List, Iterable, Tuple

from sqlalchemy import (
    Column, Integer, String, DateTime, Numeric, Text, ForeignKey
)
from sqlalchemy.orm import declarative_base, relationship, Session


# ======================================================
# МОДЕЛИ
# ======================================================

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


@dataclass
class ParsedPayment:
    date: datetime
    amount: float
    description: str
    sender_info: Optional[str]
    guessed_apartment_number: Optional[int] = None
    apartment_id: Optional[int] = None


# ======================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ======================================================

def normalize(name: str) -> str:
    """Убирает пробелы/неразрывные пробелы/нижний регистр."""
    if not isinstance(name, str):
        return ""
    return name.replace("\xa0", " ").strip().lower()


def find_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Поиск подходящей колонки."""
    norm_candidates = [c.lower() for c in candidates]
    for col in df.columns:
        if normalize(col) in norm_candidates:
            return col
    return None


def detect_sender_column(df: pd.DataFrame) -> Optional[str]:
    """
    Определяем колонку, где находится блок отправителя.
    Ищем знакомые элементы: Сбербанк, //, КВ, адресные признаки.
    """
    keywords = ["сбербанк", "//", "россия", "ул", "кв", "корп"]

    for col in df.columns:
        series = df[col].dropna().astype(str).str.lower()
        match_count = sum(any(kw in row for kw in keywords) for row in series.head(10))
        if match_count >= 1:
            return col

    return None


# ======================================================
# ЧТЕНИЕ И НОРМАЛИЗАЦИЯ ВЫПИСКИ
# ======================================================

def read_sber_statement_excel(path: str) -> pd.DataFrame:
    df = pd.read_excel(path, header=9, dtype=str)

    print("FACTUAL COLUMNS:", df.columns.tolist())

    col_date = find_column(df, ["дата проводки"])
    col_descr = find_column(df, ["назначение платежа"])
    col_amount = find_column(df, ["сумма"])
    col_credit = find_column(df, ["сумма по кредиту"])

    if not col_amount and col_credit:
        col_amount = col_credit

    if not col_date or not col_descr or not col_amount:
        raise ValueError(
            f"Не удалось обнаружить обязательные колонки: date={col_date}, "
            f"amount={col_amount}, descr={col_descr}"
        )

    sender_col = detect_sender_column(df)
    print("Detected sender column:", sender_col)

    df_out = pd.DataFrame()
    df_out["date"] = pd.to_datetime(df[col_date], errors="coerce")

    df_out["amount"] = (
        df[col_amount]
        .astype(str)
        .str.replace(" ", "")
        .str.replace(",", ".", regex=False)
    )
    df_out["amount"] = pd.to_numeric(df_out["amount"], errors="coerce")

    df_out["description"] = df[col_descr].astype(str)

    if sender_col:
        df_out["sender_info"] = df[sender_col].astype(str)
    else:
        df_out["sender_info"] = None

    df_out = df_out.dropna(subset=["date", "amount"])
    df_out = df_out.reset_index(drop=True)

    print("ROWS AFTER CLEAN:", len(df_out))

    return df_out


# ======================================================
# РАСПОЗНАВАНИЕ КВАРТИРЫ
# ======================================================

def detect_apartment(description: str, sender_info: Optional[str]) -> Optional[int]:
    """
    Логика определения квартиры:
    1) Приоритет: адрес отправителя, если совпадает с адресом дома ЖСК
    2) Fallback: назначение платежа
    """

    HOME_STREET = "болотниковская"
    HOME_HOUSE = "д 38"
    HOME_KORP = "корп 6"

    # ---- 1. По отправителю ----
    if sender_info:
        text = sender_info.lower()

        if HOME_STREET in text and HOME_HOUSE in text and HOME_KORP in text:
            m = re.search(r"кв[.\s]*([0-9]{1,3})", text)
            if m:
                return int(m.group(1))

    # ---- 2. По назначению ----
    if description:
        desc = description.lower().strip()

        # ;0000000063 или ;63 в конце строки
        m = re.search(r";\s*0*([0-9]{1,3})\s*$", desc)
        if m:
            return int(m.group(1))

        # кв. 63 / кв-63 / кв 63
        m = re.search(r"кв[.\s-]*([0-9]{1,3})", desc)
        if m:
            return int(m.group(1))

        # квартира 63
        m = re.search(r"квартира\s*([0-9]{1,3})", desc)
        if m:
            return int(m.group(1))

    return None


def parse_statement_to_payments(df: pd.DataFrame) -> List[ParsedPayment]:
    result = []

    for _, row in df.iterrows():
        apt = detect_apartment(row["description"], row["sender_info"])

        result.append(
            ParsedPayment(
                date=row["date"],
                amount=row["amount"],
                description=row["description"],
                sender_info=row["sender_info"],
                guessed_apartment_number=apt,
            )
        )

    return result


# ======================================================
# СВЯЗКА С КВАРТИРАМИ
# ======================================================

def attach_apartment_ids(
    payments: Iterable[ParsedPayment],
    session: Session
) -> Tuple[List[ParsedPayment], List[ParsedPayment]]:

    apartments = session.query(Apartment).all()
    mapping = {a.number: a.id for a in apartments}

    matched, unmatched = [], []

    for p in payments:
        if p.guessed_apartment_number in mapping:
            p.apartment_id = mapping[p.guessed_apartment_number]
            matched.append(p)
        else:
            unmatched.append(p)

    return matched, unmatched


# ======================================================
# СОХРАНЕНИЕ
# ======================================================

def save_payments_to_db(session: Session, matched, unmatched):
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
            raw_info=str(p.sender_info)
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
