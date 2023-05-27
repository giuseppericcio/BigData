import pandas as pd
import numpy as np
import streamlit as st
from PIL import Image
import plotly.express as px
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from pymongo import MongoClient
from hypecycle import HypeCycle as hc
import nltk
from nltk.corpus import stopwords

# image
image = Image.open("/app/bigdata/Dashboard/img/logo_blu.png")

# ---- SETTINGS PAGE ----
st.set_page_config(page_title="Report Progetti UniNa", page_icon=image, layout="wide")

# ---- SIDE BAR ----
st.sidebar.image(image, width=100)
st.sidebar.header('Progetti e ricerche della Federico II')
st.sidebar.caption(':bar_chart: Analytics in MongoDB e Neo4j di una raccolta di dati relative a tutte le ricerche della Federico II di Napoli')

def openImage():
    analytic1 = Image.open('/app/bigdata/Dashboard/img/Analytic1.png')
    analytic2 = Image.open('/app/bigdata/Dashboard/img/Analytic2.png')
    analytic8 = Image.open('/app/bigdata/Dashboard/img/Analytic8.png')

    return analytic1,analytic2,analytic8

analytic1,analytic2,analytic8 = openImage()

# ---- STOPWORDS ----
st.cache_data()
def downloadStopwords():
    # Stopwords
    nltk.download('stopwords')
    english_stopwords = set(stopwords.words('english'))

    return english_stopwords

# ---- CONNESSIONE A MONGODB ----
st.cache_data()
def connectMongo():
    ## CONNESSIONE A MONGO ATLAS
    client = MongoClient('mongodb+srv://mickey121098:Ciao1234.@cluster0.dj6d5rw.mongodb.net/')

    db = client.dataset_unina_research
    research = db.research

    return research

research = connectMongo()
english_stopwords = downloadStopwords()

# ---- MAINPAGE ----
# Crea una colonna per l'immagine e una per il titolo
st.title("📈 Analytics")
st.markdown(':notebook: HW2 :arrow_right: Analytics in MongoDB e Neo4j di una raccolta di dati relative a tutte le ricerche della Federico II di Napoli')

# ---- ANALYTICS ----
st.markdown("### Per ogni ambito di ricerca specifico (**caratterizzati da un codice identificativo a 4 cifre**) calcolare il numero di progetti, di questi si mostrano i TOP 10.")
query10tematiches = list(research.aggregate([
    {"$project": {"Fields_of_Research_ANZSRC_2020": {"$split": ["$Fields_of_Research_ANZSRC_2020", "; "]}}},
    {"$unwind": "$Fields_of_Research_ANZSRC_2020"},
    {"$match": {"Fields_of_Research_ANZSRC_2020": {"$regex": r'^\d{4} '}}},  # Filtra i documenti dove Fields_of_Research_ANZSRC_2020 inizia con 4 cifre
    {"$group": {"_id": {"$trim": {"input": "$Fields_of_Research_ANZSRC_2020"}}, "Numero_Progetti": {"$sum": 1}}},  # Raggruppa per Fields_of_Research_ANZSRC_2020 dopo aver tolto gli spazi bianchi e conta
    {"$project": {"_id": 0, "Ambito_di_Ricerca": "$_id", "Numero_Progetti": "$Numero_Progetti"}},
    {"$sort": {"Numero_Progetti": -1}},  # Ordina in ordine decrescente per Numero_Progetti
    {"$limit": 10}
]))

# Creare una lista di ambiti di ricerca e numero di progetti dai risultati della query
df = pd.DataFrame(query10tematiches)

fig = px.bar(df, x='Ambito_di_Ricerca', y='Numero_Progetti')
fig.update_layout(yaxis={'categoryorder':'total ascending'})

st.plotly_chart(fig, use_container_width=True)

st.markdown("In accordo con il bar chart si effettua la medesima analisi con il supporto visivo di un grafo estrapolato da Neo4j.")
st.image(analytic2)


st.markdown("### Per ogni ambito di ricerca generico (**caratterizzati da un codice identificativo a 2 cifre**) calcolare il numero di progetti, di questi si mostrano i TOP 10.")
query10tematicheg = list(research.aggregate([
    {"$project": {"Fields_of_Research_ANZSRC_2020": {"$split": ["$Fields_of_Research_ANZSRC_2020", "; "]}}},
    {"$unwind": "$Fields_of_Research_ANZSRC_2020"},
    {"$match": {"Fields_of_Research_ANZSRC_2020": {"$regex": r'^\d{2} '}}},  # Filtra i documenti dove Fields_of_Research_ANZSRC_2020 inizia con 2 cifre
    {"$group": {"_id": {"$trim": {"input": "$Fields_of_Research_ANZSRC_2020"}}, "Numero_Progetti": {"$sum": 1}}},  # Raggruppa per Fields_of_Research_ANZSRC_2020 dopo aver tolto gli spazi bianchi e conta
    {"$project": {"_id": 0, "Ambito_di_Ricerca": "$_id", "Numero_Progetti": "$Numero_Progetti"}},
    {"$sort": {"Numero_Progetti": -1}},  # Ordina in ordine decrescente per Numero_Progetti
    {"$limit": 10}
]))

# Creare una lista di ambiti di ricerca e numero di progetti dai risultati della query
df = pd.DataFrame(query10tematicheg)

fig = px.bar(df, x='Ambito_di_Ricerca', y='Numero_Progetti')
fig.update_layout(yaxis={'categoryorder':'total ascending'})

st.plotly_chart(fig, use_container_width=True)

st.markdown("In accordo con il bar chart si effettua la medesima analisi con il supporto visivo di un grafo estrapolato da Neo4j.")
st.image(analytic1)

lcol2, rcol2 = st.columns(2)

with lcol2:
    st.markdown("### Per ogni ambito di ricerca specifico (**caratterizzati da un codice identificativo a 4 cifre**) calcolare la somma finanziata, di questi si mostrano i TOP 10.")
    query_amb_fin_spec = list(research.aggregate([
        {"$project": {"Fields_of_Research_ANZSRC_2020": {"$split": ["$Fields_of_Research_ANZSRC_2020", "; "]}, "Funding_Amount_in_EUR": "$Funding_Amount_in_EUR"}},
        {"$unwind": "$Fields_of_Research_ANZSRC_2020"},
        {"$match": {"Fields_of_Research_ANZSRC_2020": {"$regex": r'^\d{4} '}}},
        {"$group": {"_id": {"$trim": {"input": "$Fields_of_Research_ANZSRC_2020"}}, "Somma_Finanziata_Totale": {"$sum": "$Funding_Amount_in_EUR"}}},
        {"$project": {"_id": 0, "Ambito_di_Ricerca": "$_id", "Somma_Finanziata_Totale": "$Somma_Finanziata_Totale"}},
        {"$sort": {"Somma_Finanziata_Totale": -1}},  # Ordina in ordine decrescente per Somma_Finanziata_Totale
        {"$limit": 10}
    ]))

    # Creare una lista di campi di ricerca e somme finanziate totali dai risultati della query
    df = pd.DataFrame(query_amb_fin_spec)

    fig = px.bar(df, x='Ambito_di_Ricerca', y='Somma_Finanziata_Totale')
    fig.update_layout(yaxis={'categoryorder':'total ascending'})

    st.plotly_chart(fig, use_container_width=True)

with rcol2:
    st.markdown("### Per ogni ambito di ricerca generico (**caratterizzati da un codice identificativo a 2 cifre**) calcolare la somma finanziata, di questi si mostrano i TOP 10.")
    query_amb_fin_gen = list(research.aggregate([
        {"$project": {"Fields_of_Research_ANZSRC_2020": {"$split": ["$Fields_of_Research_ANZSRC_2020", "; "]}, "Funding_Amount_in_EUR": "$Funding_Amount_in_EUR"}},
        {"$unwind": "$Fields_of_Research_ANZSRC_2020"},
        {"$match": {"Fields_of_Research_ANZSRC_2020": {"$regex": r'^\d{2} '}}},
        {"$group": {"_id": {"$trim": {"input": "$Fields_of_Research_ANZSRC_2020"}}, "Somma_Finanziata_Totale": {"$sum": "$Funding_Amount_in_EUR"}}},
        {"$project": {"_id": 0, "Ambito_di_Ricerca": "$_id", "Somma_Finanziata_Totale": "$Somma_Finanziata_Totale"}},
        {"$sort": {"Somma_Finanziata_Totale": -1}},  # Ordina in ordine decrescente per Somma_Finanziata_Totale
        {"$limit": 10}
    ]))

    # Creare una lista di campi di ricerca e somme finanziate totali dai risultati della query
    df = pd.DataFrame(query_amb_fin_gen)

    fig = px.bar(df, x='Ambito_di_Ricerca', y='Somma_Finanziata_Totale')
    fig.update_layout(yaxis={'categoryorder':'total ascending'})

    st.plotly_chart(fig, use_container_width=True)

st.markdown("### Per i primi 5 ambiti di ricerca specifici (**caratterizzati da un codice identificativo a 4 cifre**) trovati dalla **Analytic 1** viene effettuato il **wordcount** sul topic di ricerca estratto nella fase **Raccolta dati** a partire dal titolo del progetto con le API di OpenAI.")
st.markdown("**3101 Biochemistry and Cell Biology**")
query_wc_Bio = list(research.aggregate([
    {"$project": {"Fields_of_Research_ANZSRC_2020": {"$split": ["$Fields_of_Research_ANZSRC_2020", "; "]}, "Topic_words": {"$split": ["$topic", " "]}}},
    {"$unwind": "$Fields_of_Research_ANZSRC_2020", "$unwind": "$Topic_words"},
    {"$match": {"Fields_of_Research_ANZSRC_2020": {"$regex": r'^\d{4} '}}},
    {"$match": {"Fields_of_Research_ANZSRC_2020": "3101 Biochemistry and Cell Biology"}},
    {"$match": {"Topic_words": {"$nin": list(english_stopwords)}}},  # Filtra le parole che non sono nelle stopwords
    {"$match": {"Topic_words": {"$nin": ['&',',']}}},
    {"$group": {"_id": {"$toLower": "$Topic_words"}, "count": {"$sum": 1}}},
    {"$project": {"_id": 0, "Word": "$_id", "Count": "$count"}},
    {"$sort": {"Count": -1}},
    {"$limit": 20}
]))

# Creare un dizionario di parole e conteggi dai risultati della query
word_counts = {item['Word']: item['Count'] for item in query_wc_Bio}

# Creare un oggetto WordCloud con pesi basati sulla frequenza delle parole
wordcloud = WordCloud(width=800, height=400, background_color='white', prefer_horizontal=0.7).generate_from_frequencies(word_counts)

# Visualizza il Word Cloud
plt.figure(figsize=(8,5))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.margins(0,0)

st.pyplot(plt)

lcol3, rcol3 = st.columns(2)

with lcol3:
    st.markdown("**3105 Genetics**")
    query_wc_Genetics = list(research.aggregate([
        {"$project": {"Fields_of_Research_ANZSRC_2020": {"$split": ["$Fields_of_Research_ANZSRC_2020", "; "]}, "Topic_words": {"$split": ["$topic", " "]}}},
        {"$unwind": "$Fields_of_Research_ANZSRC_2020", "$unwind": "$Topic_words"},
        {"$match": {"Fields_of_Research_ANZSRC_2020": {"$regex": r'^\d{4} '}}},
        {"$match": {"Fields_of_Research_ANZSRC_2020": "3105 Genetics"}},
        {"$match": {"Topic_words": {"$nin": list(english_stopwords)}}},  # Filtra le parole che non sono nelle stopwords
        {"$match": {"Topic_words": {"$nin": ['&',',']}}},
        {"$group": {"_id": {"$toLower": "$Topic_words"}, "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "Word": "$_id", "Count": "$count"}},
        {"$sort": {"Count": -1}},
        {"$limit": 20}
    ]))

    # Creare un dizionario di parole e conteggi dai risultati della query
    word_counts = {item['Word']: item['Count'] for item in query_wc_Genetics}

    # Creare un oggetto WordCloud con pesi basati sulla frequenza delle parole
    wordcloud = WordCloud(width=800, height=400, background_color='white', prefer_horizontal=0.7).generate_from_frequencies(word_counts)

    # Visualizza il Word Cloud
    plt.figure(figsize=(8,2.5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.margins(0,0)

    st.pyplot(plt)

with rcol3:
    st.markdown("**3211 Oncology and Carcinogenesis**")
    query_wc_Onc = list(research.aggregate([
        {"$project": {"Fields_of_Research_ANZSRC_2020": {"$split": ["$Fields_of_Research_ANZSRC_2020", "; "]}, "Topic_words": {"$split": ["$topic", " "]}}},
        {"$unwind": "$Fields_of_Research_ANZSRC_2020", "$unwind": "$Topic_words"},
        {"$match": {"Fields_of_Research_ANZSRC_2020": {"$regex": r'^\d{4} '}}},
        {"$match": {"Fields_of_Research_ANZSRC_2020": "3211 Oncology and Carcinogenesis"}},
        {"$match": {"Topic_words": {"$nin": list(english_stopwords)}}},  # Filtra le parole che non sono nelle stopwords
        {"$match": {"Topic_words": {"$nin": ['&',',']}}},
        {"$group": {"_id": {"$toLower": "$Topic_words"}, "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "Word": "$_id", "Count": "$count"}},
        {"$sort": {"Count": -1}},
        {"$limit": 20}
    ]))

    # Creare un dizionario di parole e conteggi dai risultati della query
    word_counts = {item['Word']: item['Count'] for item in query_wc_Onc}

    # Creare un oggetto WordCloud con pesi basati sulla frequenza delle parole
    wordcloud = WordCloud(width=800, height=400, background_color='white', prefer_horizontal=0.7).generate_from_frequencies(word_counts)

    # Visualizza il Word Cloud
    plt.figure(figsize=(8,2.5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.margins(0,0)

    st.pyplot(plt)

lcol4, rcol4 = st.columns(2)

with lcol4:
    st.markdown("**3202 Clinical Sciences**")
    query_wc_Clinical = list(research.aggregate([
        {"$project": {"Fields_of_Research_ANZSRC_2020": {"$split": ["$Fields_of_Research_ANZSRC_2020", "; "]}, "Topic_words": {"$split": ["$topic", " "]}}},
        {"$unwind": "$Fields_of_Research_ANZSRC_2020", "$unwind": "$Topic_words"},
        {"$match": {"Fields_of_Research_ANZSRC_2020": {"$regex": r'^\d{4} '}}},
        {"$match": {"Fields_of_Research_ANZSRC_2020": "3202 Clinical Sciences"}},
        {"$match": {"Topic_words": {"$nin": list(english_stopwords)}}},  # Filtra le parole che non sono nelle stopwords
        {"$match": {"Topic_words": {"$nin": ['&',',']}}},
        {"$group": {"_id": {"$toLower": "$Topic_words"}, "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "Word": "$_id", "Count": "$count"}},
        {"$sort": {"Count": -1}},
        {"$limit": 20}
    ]))

    # Creare un dizionario di parole e conteggi dai risultati della query
    word_counts = {item['Word']: item['Count'] for item in query_wc_Clinical}

    # Creare un oggetto WordCloud con pesi basati sulla frequenza delle parole
    wordcloud = WordCloud(width=800, height=400, background_color='white', prefer_horizontal=0.7).generate_from_frequencies(word_counts)

    # Visualizza il Word Cloud
    plt.figure(figsize=(8,2.5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.margins(0,0)

    st.pyplot(plt)

with rcol4:
    st.markdown("**4303 Historical Studies**")
    query_wc_Historical = list(research.aggregate([
        {"$project": {"Fields_of_Research_ANZSRC_2020": {"$split": ["$Fields_of_Research_ANZSRC_2020", "; "]}, "Topic_words": {"$split": ["$topic", " "]}}},
        {"$unwind": "$Fields_of_Research_ANZSRC_2020", "$unwind": "$Topic_words"},
        {"$match": {"Fields_of_Research_ANZSRC_2020": {"$regex": r'^\d{4} '}}},
        {"$match": {"Fields_of_Research_ANZSRC_2020": "4303 Historical Studies"}},
        {"$match": {"Topic_words": {"$nin": list(english_stopwords)}}},  # Filtra le parole che non sono nelle stopwords
        {"$match": {"Topic_words": {"$nin": ['&',',']}}},
        {"$group": {"_id": {"$toLower": "$Topic_words"}, "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "Word": "$_id", "Count": "$count"}},
        {"$sort": {"Count": -1}},
        {"$limit": 20}  # Limita a 10 documenti
    ]))

    # Creare un dizionario di parole e conteggi dai risultati della query
    word_counts = {item['Word']: item['Count'] for item in query_wc_Historical}

    # Creare un oggetto WordCloud con pesi basati sulla frequenza delle parole
    wordcloud = WordCloud(width=800, height=400, background_color='white', prefer_horizontal=0.7).generate_from_frequencies(word_counts)

    # Visualizza il Word Cloud
    plt.figure(figsize=(8,2.5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.margins(0,0)

    st.pyplot(plt)

st.markdown("### Evoluzione temporale dei Progetti per Ambito di Ricerca della Federico II")
queryTemporalProject = list(research.aggregate([ 
    {"$match": {"Start_Year": {"$ne": None}}},
    {"$project": {"Start_Year": "$Start_Year", "Fields_of_Research_ANZSRC_2020": {"$split": ["$Fields_of_Research_ANZSRC_2020", "; "]}}},
    {"$unwind": "$Fields_of_Research_ANZSRC_2020"}, {"$match": {"Fields_of_Research_ANZSRC_2020": {"$regex": r'^\d{4} '}}},
    {"$match": {"Fields_of_Research_ANZSRC_2020": {"$in": ["3101 Biochemistry and Cell Biology", "3105 Genetics", "3211 Oncology and Carcinogenesis", "3202 Clinical Sciences", "4303 Historical Studies" ]}}},
    {"$group": {"_id": {"Year": "$Start_Year", "Fields_of_Research_ANZSRC_2020": "$Fields_of_Research_ANZSRC_2020"}, "Numero_Progetti": {"$sum": 1}}},
    {"$project": {"_id": 0, "Anno": "$_id.Year", "Ambito_di_Ricerca": "$_id.Fields_of_Research_ANZSRC_2020", "Numero_Progetti": "$Numero_Progetti"}},
    {"$sort": {"Anno": 1, "Ambito_di_Ricerca": 1}}
])) 

# Converti i dati in un DataFrame di Pandas
df = pd.DataFrame(queryTemporalProject)

fig = px.line(df, x="Anno", y="Numero_Progetti", color = 'Ambito_di_Ricerca')
fig.update_layout(yaxis={'categoryorder':'total ascending'})

st.plotly_chart(fig, use_container_width=True)

st.markdown("### Variazione temporale (**per ogni anno**) del numero di progetti per ciascun Ambito di Ricerca specifico attraverso un **Bar Chart Race**.")
video_file = open('/app/bigdata/Dashboard/filmati/ambitiricercaprogetti.mp4', 'rb')
video_bytes = video_file.read()

st.video(video_bytes)

st.markdown("### Variazione temporale (**per ogni anno**) della somma finanziata per ciascun Ambito di Ricerca specifico attraverso un **Bar Chart Race**.")
video_file = open('/app/bigdata/Dashboard/filmati/ambitiricercasommafinanziata.mp4', 'rb')
video_bytes = video_file.read()

st.video(video_bytes)

st.markdown("### 'Objective Cycle' degli Obiettivi Sostenibili della Federico II")
querySustanaibleGoals = list(research.aggregate([
    {"$project": {"Sustainable_Development_Goals": {"$split": ["$Sustainable_Development_Goals", "; "]}}},
    {"$unwind": "$Sustainable_Development_Goals"},
    {"$group": {"_id": {"$trim": {"input": "$Sustainable_Development_Goals"}}, "Numero_Progetti": {"$sum": 1}}},
    {"$project": {"_id": 0, "Obiettivi_Sostenibili": "$_id", "Numero_Progetti": "$Numero_Progetti"}},
    {"$sort": {"Numero_Progetti": -1, "Obiettivi_Sostenibili": -1}},
    {"$limit": 10}
]))

# Converti i dati in un DataFrame di Pandas
df = pd.DataFrame(querySustanaibleGoals)

plt.figure(figsize=(15, 5))  # Aggiungi questa linea per allargare il grafico
plt.xlabel("Numero progetti")
plt.ylabel("Obiettivo perseguito")

ax = plt.gca()
ax.axes.yaxis.set_ticklabels([])

x = np.linspace(df['Numero_Progetti'].min(), df['Numero_Progetti'].max(), 5000)

# Aggiungere obiettivi all'Objective Cycle
for idx,row in df.iterrows():
    if row.Numero_Progetti in range(0,30):
        hc.annotate(x, x_value = row.Numero_Progetti, 
                text = row.Obiettivi_Sostenibili, 
                color="black", fontsize=10, rotation=0, 
                fontproperties='Arial')
    else:
        hc.annotate(x, x_value = row.Numero_Progetti, 
                text = row.Obiettivi_Sostenibili, 
                color="black", fontsize=10, rotation=25, 
                fontproperties='Arial')

# Visualizzare il grafico
plt = hc.visualize(x,"red")
st.pyplot(plt)

st.markdown("In accordo con l'objective cycle si effettua la medesima analisi con il supporto visivo di un grafo estrapolato da Neo4j.")
st.image(analytic8)


# ---- HIDE STREAMLIT STYLE ----
hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)
