import streamlit as st
from PIL import Image
import wikipediaapi

# ---- READ LOGO ----
image = Image.open("/app/bigdata/Dashboard/img/logo_blu.png")

# ---- SETTINGS PAGE ----
st.set_page_config(page_title="Report Progetti UniNa", page_icon=image, layout="wide")

# ---- SIDE BAR ----
st.sidebar.image(image, width=100)
st.sidebar.header('Progetti e ricerche della Federico II')
st.sidebar.caption(':bar_chart: In occasione degli 800 anni della Federico II si raccolgono informazioni sui progetti e ricerche della Università Federico II di Napoli.')

# ---- RETRIEVAL DELLA PAGINA DELL'UNIVERSITA' DA WIKIPEDIA ----
@st.cache_data
def retrieval_page_wikipedia():
    wiki = wikipediaapi.Wikipedia('it')
    page = wiki.page('Università degli Studi di Napoli Federico II')

    return page

federicoii = retrieval_page_wikipedia()


# ---- MAINPAGE ----
# Crea una colonna per l'immagine e una per il titolo
lcol_title, rcol_title = st.columns([.9, 9.1])

# Mostra l'immagine nella prima colonna
st.title("Progetti e ricerche della Federico II")
st.write(':bar_chart: In occasione degli 800 anni della Federico II si raccolgono informazioni sui progetti e ricerche della Università Federico II di Napoli.')

# Aggiunge un po' di storia sull'Università Federico II
st.header('Storia della Università degli Studi di Napoli "Federico II"')
st.info(federicoii.summary)
st.markdown('Fonte: Wikipedia')


# ---- HIDE STREAMLIT STYLE ----
hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)
