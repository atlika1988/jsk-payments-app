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


# ---------------------------------------------------------
# 1. НАСТРОЙКА БАЗЫ
# ---------------------------------------------------------

DATABASE_URL = "sqlite:///jsk.db"

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)
Base.metadata.create_all(engine)


# ---------------------------------------------------------
# 2. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ---------------------------------------------------------

def payments_to_dataframe(payments):
    """Преобразует список ParsedPayment в табличку для отображения."""
    return pd.DataFrame([
        {
            "Дата": p.date.strftime("%Y-%m-%d"),
            "Сумма": p.amount,
            "Описание": p.description,
            "Угадано кв.": p.guessed_apartment_number,
            "Выбрана кв.": p.apartment_id,
        }
        for p in payments
    ])


def load_apartment_map(session):
    """Возвращает dict: {id: 12, number: 12} для выпадающего списка."""
    apartments = session.query(Apartment).order_by(Apartment.number).all()
    return {f"Кв {a.number}": a.id for a in apartments}


# ---------------------------------------------------------
# 3. UI Streamlit
# ---------------------------------------------------------

st.title("📄 Импорт выписки СберБизнес — ЖСК")

session = SessionLocal()

# Карта квартир
apt_map = load_apartment_map(session)   # {'Кв 1': 1, 'Кв 2': 2, ...}


st.header("1. Загрузка файла выписки")

uploaded = st.file_uploader("Загрузите файл Excel (.xlsx)", type=["xlsx"])
if uploaded:
    # Сначала сохраняем файл временно
    temp_path = "uploaded_file.xlsx"
    with open(temp_path, "wb") as f:
        f.write(uploaded.read())

    st.success("Файл загружен. Обрабатываю…")

    # Импортируем
    matched, unmatched = import_statement(temp_path, session)

    st.subheader("2. Автоматически опознанные платежи")
    df_matched = payments_to_dataframe(matched)
    st.dataframe(df_matched, use_container_width=True)

    st.subheader("3. Неопознанные платежи — требуется выбор квартиры")

    # Преобразуем в удобный формат
    df_unmatched = payments_to_dataframe(unmatched)

    # Место хранить выборы пользователя
    selected_apartments = {}

    # Для каждой строки — выводим описание и выпадающее меню
    for i, p in enumerate(unmatched):
        with st.expander(f"Платёж №{i+1} — {p.amount} ₽ • {p.date.date()}"):
            st.write(f"**Описание:** {p.description}")
            st.write(f"**Автопоиск:** {p.guessed_apartment_number}")

            choice = st.selectbox(
                f"Выберите квартиру:",
                ["Не выбрано"] + list(apt_map.keys()),
                key=f"apt_choice_{i}"
            )

            if choice != "Не выбрано":
                p.apartment_id = apt_map[choice]  # сохраняем id в объекте
                selected_apartments[i] = apt_map[choice]

    # Кнопка сохранения
    if st.button("📌 Провести платежи"):
        # Финально разделим
        final_matched = []
        final_unmatched = []

        for p in unmatched:
            if p.apartment_id:
                final_matched.append(p)
            else:
                final_unmatched.append(p)

        # Добавляем уже опознанных раньше
        final_matched.extend(matched)

        # Сохраняем в БД
        save_payments_to_db(session, final_matched, final_unmatched)

        st.success("Платежи успешно сохранены!")
        st.info(f"Опознано платежей: {len(final_matched)}")
        st.info(f"Неопознанных сохранено: {len(final_unmatched)}")

        st.balloons()
