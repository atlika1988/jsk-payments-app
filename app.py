import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from io import BytesIO

from payments_import import (
    import_statement,
    save_payments_to_db,
    Base,
    Apartment,
)

from charges import generate_charges, ChargeRow


# ======================================================
# Глобальные настройки интерфейса
# ======================================================
st.set_page_config(
    page_title="ЖСК Руслан",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Принудительная светлая тема (CSS override)
st.markdown("""
<style>
:root {
    color-scheme: light !important;
}
[data-testid="stAppViewContainer"] {
    background-color: #ffffff !important;
}
</style>
""", unsafe_allow_html=True)


# ======================================================
# База данных
# ======================================================

DATABASE_URL = "sqlite:///jsk.db"
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)
Base.metadata.create_all(engine)


# ======================================================
# Табличные утилиты
# ======================================================

def payments_to_dataframe(payments):
    return pd.DataFrame([
        {
            "Дата": p.date.strftime("%Y-%m-%d"),
            "Сумма": float(p.amount),
            "Описание": p.description,
            "Отправитель": p.sender_info,
            "Автоопределение": p.guessed_apartment_number,
            "Квартира ID": p.apartment_id,
        }
        for p in payments
    ])


def charges_to_df(rows: list[ChargeRow]):
    return pd.DataFrame([
        {
            "Квартира": r.apartment_number,
            "Услуга": r.item_name,
            "Код": r.item_code,
            "Период": r.period.strftime("%Y-%m-%d"),
            "Сумма": r.amount,
        }
        for r in rows
    ])


def excel_bytes_from_df(dfs: dict):
    """Создаёт Excel с несколькими листами."""
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine="openpyxl")

    for sheet, df in dfs.items():
        df.to_excel(writer, sheet_name=sheet, index=False)

        ws = writer.book[sheet]

        # автоширина
        for col in ws.columns:
            max_len = max(len(str(c.value)) if c.value else 0 for c in col)
            ws.column_dimensions[col[0].column_letter].width = max_len + 2

        ws.auto_filter.ref = ws.dimensions
        ws.freeze_panes = "A2"

    writer.close()
    output.seek(0)
    return output


# ======================================================
# Sidebar — меню
# ======================================================

st.sidebar.title("Меню")
mode = st.sidebar.radio("Раздел:", ["Платежи", "Начисления"])


# ======================================================
# РАЗДЕЛ 1 — ПЛАТЕЖИ
# ======================================================

if mode == "Платежи":

    session = SessionLocal()

    st.header("📄 Импорт выписки СберБизнес")

    uploaded = st.file_uploader("Выберите файл .xlsx", type=["xlsx"])

    if uploaded:
        temp_path = "uploaded_statement.xlsx"
        with open(temp_path, "wb") as f:
            f.write(uploaded.read())

        st.success("Файл загружен. Обрабатываю…")

        matched, unmatched = import_statement(temp_path, session)

        # ---------- автоматические ----------
        st.subheader("Автоматически распознанные платежи")
        st.dataframe(payments_to_dataframe(matched), use_container_width=True)

        # ---------- ручное распределение ----------
        st.subheader("Платежи, требующие ручного сопоставления")

        apt_map = {f"Кв {a.number}": a.id for a in session.query(Apartment).all()}

        cols = st.columns([1, 1, 3, 1, 2])
        cols[0].markdown("**Дата**")
        cols[1].markdown("**Сумма**")
        cols[2].markdown("**Описание**")
        cols[3].markdown("**Авто**")
        cols[4].markdown("**Квартира**")

        for idx, p in enumerate(unmatched):
            row = st.columns([1, 1, 3, 1, 2])
            row[0].write(p.date.strftime("%Y-%m-%d"))
            row[1].write(float(p.amount))
            row[2].write(p.description)
            row[3].write(p.guessed_apartment_number)

            choice = row[4].selectbox(
                "",
                ["Не выбрано"] + list(apt_map.keys()),
                key=f"apt_choice_{idx}"
            )

            if choice != "Не выбрано":
                p.apartment_id = apt_map[choice]

        if st.button("📌 Провести платежи"):

            final_matched = matched + [p for p in unmatched if p.apartment_id]
            final_unmatched = [p for p in unmatched if not p.apartment_id]

            save_payments_to_db(session, final_matched, final_unmatched)
            st.success("Платежи сохранены!")

        # ------- отчёт -------
        st.download_button(
            "📥 Скачать отчёт",
            data=excel_bytes_from_df({
                "Распознанные": payments_to_dataframe(matched),
                "Нераспознанные": payments_to_dataframe(unmatched),
            }),
            file_name="Отчёт_платежей.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


# ======================================================
# РАЗДЕЛ 2 — НАЧИСЛЕНИЯ
# ======================================================

elif mode == "Начисления":

    st.header("🧮 Расчёт начислений")

    session = SessionLocal()

    year = st.number_input("Год:", min_value=2023, max_value=2035, value=2025)
    month = st.selectbox(
        "Месяц:",
        ["01","02","03","04","05","06","07","08","09","10","11","12"],
        index=1
    )

    if st.button("📌 Рассчитать начисления"):
        rows = generate_charges(session, int(year), int(month))

        st.success("Начисления успешно рассчитаны!")

        df_charges = charges_to_df(rows)
        st.dataframe(df_charges, use_container_width=True)

        st.download_button(
            "📥 Скачать начисления",
            data=excel_bytes_from_df({"Начисления": df_charges}),
            file_name=f"Начисления_{year}-{month}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
