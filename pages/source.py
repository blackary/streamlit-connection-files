from pathlib import Path

import streamlit as st

code = Path("fsspec_connection.py").read_text()
st.code(code, language="python")
