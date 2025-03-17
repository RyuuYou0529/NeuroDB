import streamlit as st
import datajoint as dj

st.title("Homepage")

db_name_selected = st.selectbox("Database Name", st.session_state.GLOBAL.db_name_list, index=None)

button_col1, button_col2 = st.columns(2)
with button_col1:
    if st.button("🔗 confirm"):
        st.session_state.GLOBAL.switch_db(db_name_selected)
        st.switch_page("dashboard.py")
with button_col2:
    if st.button("🔄 Refresh"):
        st.session_state.GLOBAL.update_db_name_list()

st.write(f"Working with database: **{st.session_state.GLOBAL.db_name_selected}**")