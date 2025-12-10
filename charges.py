# charges.py

from __future__ import annotations
from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Dict

from sqlalchemy import (
    Column, Integer, String, Date, DateTime, Numeric, Float, ForeignKey
)
from sqlalchemy.orm import relationship, Session

from payments_import import Base, Apartment  # используем существующую модель Apartment


# =========================================================
# МОДЕЛИ: TariffItem, Charge
# =========================================================

class TariffItem(Base):
    """
    Историчные тарифы:
    - target_fee    (целевой взнос на м²)
    - radio
    - antenna
    - intercom_small
    - intercom_big
    - bank_percent  (%)
    """
    __tablename__ = "tariff_items"

    id = Column(Integer, primary_key=True)
    code = Column(String, nullable=False, index=True)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)       # 'per_m2', 'fixed', 'percent'
    value = Column(Float, nullable=False)
    valid_from = Column(Date, nullable=False, index=True)


class Charge(Base):
    """
    Строка начисления за месяц по квартире.
    """
    __tablename__ = "charges"

    id = Column(Integer, primary_key=True)
    apartment_id = Column(Integer, ForeignKey("apartments.id"), nullable=False)
    period = Column(Date, nullable=False, index=True)
    item_code = Column(String, nullable=False)
    item_name = Column(String, nullable=False)
    amount = Column(Numeric(12, 2), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    apartment = relationship("Apartment")


# =========================================================
# УТИЛИТЫ
# =========================================================

@dataclass
class ChargeRow:
    apartment_id: int
    apartment_number: int
    period: date
    item_code: str
    item_name: str
    amount: float


def month_start(year: int, month: int) -> date:
    return date(year, month, 1)


def get_active_tariffs(session: Session, period: date) -> Dict[str, TariffItem]:
    """
    Возвращает самые свежие тарифы с valid_from <= period.
    """
    items = session.query(TariffItem).all()
    result: Dict[str, TariffItem] = {}

    for t in items:
        if t.valid_from <= period:
            prev = result.get(t.code)
            if prev is None or prev.valid_from < t.valid_from:
                result[t.code] = t

    return result


# =========================================================
# РАСЧЁТ НАЧИСЛЕНИЙ ДЛЯ ОДНОЙ КВАРТИРЫ
# =========================================================

def calculate_charges_for_apartment(
    apt: Apartment,
    period: date,
    tariffs: Dict[str, TariffItem],
) -> List[ChargeRow]:

    rows: List[ChargeRow] = []
    total_before_percent = 0.0  # для банковского процента

    # -------------------
    # 1. Целевой взнос
    # -------------------
    t_target = tariffs.get("target_fee")
    if t_target and apt.area:
        amount = float(apt.area) * float(t_target.value)
        total_before_percent += amount

        rows.append(ChargeRow(
            apartment_id=apt.id,
            apartment_number=apt.number,
            period=period,
            item_code="target_fee",
            item_name=t_target.name,
            amount=round(amount, 2),
        ))

    # -------------------
    # 2. Радио
    # -------------------
    t_radio = tariffs.get("radio")
    if t_radio and apt.radio:
        coeff = float(apt.radio)  # 0 / 0.5 / 1
        amount = float(t_radio.value) * coeff
        total_before_percent += amount

        rows.append(ChargeRow(
            apartment_id=apt.id,
            apartment_number=apt.number,
            period=period,
            item_code="radio",
            item_name=t_radio.name,
            amount=round(amount, 2),
        ))

    # -------------------
    # 3. Антенна
    # -------------------
    t_antenna = tariffs.get("antenna")
    if t_antenna and apt.antenna:
        coeff = float(apt.antenna)
        amount = float(t_antenna.value) * coeff
        total_before_percent += amount

        rows.append(ChargeRow(
            apartment_id=apt.id,
            apartment_number=apt.number,
            period=period,
            item_code="antenna",
            item_name=t_antenna.name,
            amount=round(amount, 2),
        ))

    # -------------------
    # 4. Домофон
    # -------------------
    if apt.intercom and apt.intercom > 0:
        amount = float(apt.intercom)
        total_before_percent += amount

        rows.append(ChargeRow(
            apartment_id=apt.id,
            apartment_number=apt.number,
            period=period,
            item_code="intercom",
            item_name="Домофон",
            amount=round(amount, 2),
        ))

    # -------------------
    # 5. Банковский процент (от ВСЕХ услуг)
    # -------------------
    t_bank = tariffs.get("bank_percent")
    if t_bank and total_before_percent > 0:
        percent = float(t_bank.value) / 100.0
        amount = total_before_percent * percent

        rows.append(ChargeRow(
            apartment_id=apt.id,
            apartment_number=apt.number,
            period=period,
            item_code="bank_percent",
            item_name=t_bank.name,
            amount=round(amount, 2),
        ))

    return rows


# =========================================================
# ГЛАВНАЯ ФУНКЦИЯ РАСЧЁТА
# =========================================================

def generate_charges(
    session: Session,
    year: int,
    month: int,
    save_to_db: bool = True,
) -> List[ChargeRow]:

    period = month_start(year, month)
    tariffs = get_active_tariffs(session, period)

    apartments = session.query(Apartment).order_by(Apartment.number).all()

    all_rows: List[ChargeRow] = []

    for apt in apartments:
        rows = calculate_charges_for_apartment(apt, period, tariffs)
        all_rows.extend(rows)

    if save_to_db:
        # очищаем старые начисления за период
        session.query(Charge).filter(Charge.period == period).delete()

        # сохраняем новые
        for r in all_rows:
            session.add(Charge(
                apartment_id=r.apartment_id,
                period=r.period,
                item_code=r.item_code,
                item_name=r.item_name,
                amount=r.amount,
            ))

        session.commit()

    return all_rows


# Отладочный запуск
if __name__ == "__main__":
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    engine = create_engine("sqlite:///jsk.db")
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    Base.metadata.create_all(engine)

    rows = generate_charges(session, 2025, 2)
    print("Строк начислений:", len(rows))
