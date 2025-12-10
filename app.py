import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from payments_import import (
    import_statement,
    save_payments_to_db,
    Base,
    Apartment,
    ParsedPayment,
)

# ======================================================
# БАЗА ДАННЫХ
# ======================================================

DATABASE_URL = "sqlite:///jsk.db"
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)
Base.metadata.create_all(engine)


def payments_to_dataframe(payments):
    return pd.DataFrame([
        {
            "Дата": p.date.strftime("%Y-%m-%d"),
            "Сумма": float(p.amount),
            "Описание": p.description,
            "Отправитель": p.sender_info,
            "Угадана кв.": p.guessed_apartment_number,
            "Выбрана кв.": p.apartment_id,
        }
        for p in payments
    ])


def load_apartment_map(session):
    apts = session.query(Apartment).order_by(Apartment.number).all()
    return {f"Кв {a.number}": a.id for a in apts}


# ======================================================
# UI
# ======================================================

st.title("📄 Импорт выписки СберБизнес — ЖСК")

session = SessionLocal()
apt_map = load_apartment_map(session)

st.header("1. Загрузка файла")
uploaded = st.file_uploader("Выберите файл .xlsx", type=["xlsx"])

if uploaded:
    temp_path = "uploaded_file.xlsx"
    with open(temp_path, "wb") as f:
        f.write(uploaded.read())

    st.success("Файл загружен. Обрабатываю...")

    matched, unmatched = import_statement(temp_path, session)

    st.subheader("2. Автоматически распознанные платежи")
    st.dataframe(payments_to_dataframe(matched), use_container_width=True)

    st.subheader("3. Платежи, требующие ручного сопоставления")

    for i, p in enumerate(unmatched):
        with st.expander(f"Платеж №{i+1} — {p.amount} ₽, {p.date.date()}"):
            st.write(f"**Описание:** {p.description}")
            st.write(f"**Отправитель:** {p.sender_info}")
            st.write(f"**Автоопределение:** {p.guessed_apartment_number}")

            choice = st.selectbox(
                "Выберите квартиру:",
                ["Не выбрано"] + list(apt_map.keys()),
                key=f"apt_sel_{i}"
            )
            if choice != "Не выбрано":
                p.apartment_id = apt_map[choice]

    if st.button("📌 Провести платежи"):
        final_matched = matched + [p for p in unmatched if p.apartment_id]
        final_unmatched = [p for p in unmatched if not p.apartment_id]

        save_payments_to_db(session, final_matched, final_unmatched)

        st.success("Платежи успешно сохранены!")
        st.balloons()
