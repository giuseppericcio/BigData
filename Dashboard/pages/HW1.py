import pandas as pd
import streamlit as st
from PIL import Image
import plotly.express as px
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import os

# image
image = Image.open("Dashboard/img/logo_blu.png")

# ---- SETTINGS PAGE ----
st.set_page_config(page_title="Report Progetti UniNa", page_icon=image, layout="wide")

# ---- SIDE BAR ----
st.sidebar.image(image, width=100)
st.sidebar.header('Progetti e ricerche della Federico II')
st.sidebar.caption(':bar_chart: Analytics in HIVE, Pig e PySpark di una raccolta di dati relative a tutte le ricerche della Federico II di Napoli')

# ---- READ CSV ----
@st.cache_data
def get_data_from_csv():
    # Directory contenente i file CSV
    directory = 'Dashboard/risultati'

    # Dizionario per salvare i dataframe
    dfs = {}

    # Leggi tutti i file CSV nella directory
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            # Crea un nome univoco per il dataframe
            name = os.path.splitext(filename)[0]
            # Leggi il file CSV e crea un dataframe
            if name == 'capacitafondi' or name == 'ricercadurata' or name == 'Settori_Medici' or name == 'ProgFondi2020':
                df = pd.read_csv(os.path.join(directory, filename),usecols=[1,2,3])
            else:
                df = pd.read_csv(os.path.join(directory, filename),usecols=[1,2])
            # Salva il dataframe nel dizionario
            dfs[name] = df
    
    return dfs

dfs = get_data_from_csv()


# ---- MAINPAGE ----
# Crea una colonna per l'immagine e una per il titolo
st.title("📈 Analytics")
st.markdown(':notebook: HW1 :arrow_right: Analytics in HIVE, Pig e PySpark di una raccolta di dati relative a tutte le ricerche della Federico II di Napoli')


# ---- ANALYTICS ----
st.markdown("### Le parole chiavi più utilizzate nei titoli dei progetti")
cont_parole = dfs['parolechiavi'].set_index('Topic_Titolo')['Conteggio'].to_dict()
# Crea un Word Cloud
wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(cont_parole)

# Visualizza il Word Cloud
plt.figure(figsize=(8,2.5))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.margins(0,0)

st.pyplot(plt)
st.markdown('I risultati ottenuti riflettono il carattere innovativo della Federico II, con parole come ”development” e ”innovative” \
            presenti tra le prime 10 parole più usate nei titoli dei progetti. Con la presenza del termine ”european”, presente quasi \
            100 volte, si conferma la centralità dell’Università in numerosi progetti in collaborazioni con istituzioni da tutta Europa. \
            Infine, è facile notare dai risultati che ”molecular” è la parola più presente di tutte, dato il grande numero di progetti in \
            campo medico e scientifico in cui è coinvolta la Federico II.')


st.markdown("### Le 100 istituzioni con cui ha collaborato di più la Federico II")
st.dataframe(dfs['Istituzioni'], use_container_width=True)
st.markdown('La tabella mostra che la Sapienza (Università di Roma) è l’istituzione con il maggior numero di collaborazioni, \
            con un totale di 476. Seguono l’Università degli Studi di Milano con 350 collaborazioni, l’Università degli Studi \
            di Firenze con 346, l’Università degli Studi di Padova con 322 e l’Università di Bologna con 317. Altre istituzioni \
            con un numero significativo di collaborazioni includono l’Università degli Studi di Torino, l’Università di Pisa, \
            l’Università degli Studi della Campania Luigi Vanvitelli.')


st.markdown("### Capacità e Fondi Media Progetti per Anno")
# Crea una figura
fig, ax1 = plt.subplots(figsize=(8,2.5))

# Definisce l'asse sinistro con i valori di Capacità Progetti
color = 'tab:blue'
ax1.set_xlabel('Anno')
ax1.set_ylabel('Capacità Progetti', color=color)
ax1.plot(dfs['capacitafondi'].Anno_Inizio, dfs['capacitafondi'].Capacita_Progetti, color=color)
ax1.tick_params(axis='y', labelcolor=color)

# Definisce l'asse destro con i valori di Fondi Media Progetti
ax2 = ax1.twinx()  # Crea un secondo asse condividendo l'asse x
color = 'tab:red'
ax2.set_ylabel('Fondi Media Progetti', color=color)
ax2.plot(dfs['capacitafondi'].Anno_Inizio, dfs['capacitafondi'].Fondi_media_Progetti, color=color)
ax2.tick_params(axis='y', labelcolor=color)

st.pyplot(plt)
st.markdown('Come si può notare dall’andamento della Capacità dei progetti (:blue[linea blu]) fino al 2010, l’Università riusciva a fare molti progetti \
            anche con poche risorse economiche a disposizione denotando una buona capacità di gestione di quest’ultime. Mentre, come si evince \
            dalla Media dei fondi per progetto (:red[linea rossa]) dopo il 2010, si è avuto un incremento di fondi impiegati in media per ogni progetto \
            e quindi, un peggioramento a parità di progetti di efficienza nella spesa delle risorse a disposizione.')




lcol2, rcol2 = st.columns(2)
with lcol2:
    st.markdown("### Numero di progetti di ricerca per ogni tipo di cancro")
    fig = px.bar(dfs['ProgettiCancro'], x='Numero_Progetti', y='Tipo_Cancro')
    fig.update_layout(yaxis={'categoryorder':'total ascending'})
    
    st.plotly_chart(fig, use_container_width=True)
    st.markdown('Dalla Figura è possibile notare che ci sono numerose patologie diverse in cui la Federico II ha lavorato su progetti di ricerca, il \
                che suggerisce che la sua forza potrebbe essere nell’avere un’ampia competenza nella ricerca oncologica. Inoltre, il fatto che la tiroide \
                e il cancro al seno siano le due patologie con il maggior numero di progetti potrebbe indicare una particolare competenza in queste aree.')


with rcol2:
    st.markdown("### TOP 10 dipartimenti con progetti ancora in corso")
    fig = px.bar(dfs['Dipartimenti'], x='Numero_Progetti_In_Corso', y='Dipartimento')
    fig.update_layout(yaxis={'categoryorder':'total ascending'})

    st.plotly_chart(fig, use_container_width=True)
    st.markdown('In Figura vengono mostrati i 10 dipartimenti con progetti ancora in corso, mostrando il numero di progetti di ricerca nei diversi \
                dipartimenti universitari. La Figura mostra che il Dipartimento di Ingegneria (B12) ha il maggior numero di progetti di ricerca attualmente \
                in corso, con un totale di 28 progetti (in accordo con i risultati precedenti). Seguono il Dipartimento di Informatica e Scienze \
                dell’Informazione (B11) con 14 progetti, la Medicina Clinica (A01) con 12, l’Agricoltura, Medicina Veterinaria e Scienze Alimentari (A06) con \
                11 e la Geografia e Studi Ambientali (C14) con 10 progetti e così via.')



lcol3, rcol3 = st.columns(2)
with lcol3:
    st.markdown("### Numero di progetti di ricerca per ogni obiettivo di sviluppo sostenibile")
    fig = px.bar(dfs['ProgettiFinanziatiSostenibilita'], x='Numero_Progetti', y='Obiettivo_Sostenibilita')
    fig.update_layout(yaxis={'categoryorder':'total ascending'})
    
    st.plotly_chart(fig, use_container_width=True)
    st.markdown('La Figura mostra la distribuzione dei progetti di sostenibilità ambientale finanziati dall’Università Federico II in base agli obiettivi di \
                sviluppo sostenibile dell’ONU. I tre progetti con il maggior numero di progetti finanziati sono Affordable and Clean Energy, Climate Action e \
                Sustainable Cities and Communities. In particolare, questi obiettivi indicano che la Federico II ha lavorato su progetti riguardanti l’energia \
                pulita, la lotta al cambiamento climatico e lo sviluppo sostenibile delle città e delle comunità. Tali progetti sono rilevanti, in quanto riguardano \
                tematiche cruciali per la sostenibilità ambientale e la qualità della vita delle persone, e richiedono l’impegno di istituzioni, aziende e \
                cittadini per raggiungere gli obiettivi stabiliti dall’Agenda 2030 delle Nazioni Unite. Da Notare: La Federico II ha finanziato circa 300mln di euro, \
                quindi ha investito nello sviluppo di infrastrutture resilienti, promuovendo l’industrializzazione sostenibile e fomentando l’innovazione nelle stesse.')
    

with rcol3:
    st.markdown("### Le 10 tematiche più trattate negli ultimi 10 anni dalla Federico II")
    fig = px.bar(dfs['Tem_Top10'], x='Numero progetti', y='Ambito di Ricerca')
    fig.update_layout(yaxis={'categoryorder':'total ascending'})
    
    st.plotly_chart(fig, use_container_width=True)
    st.markdown('Come si può notare dalla Figura sottostante, vengono presentati i dati relativi al numero di progetti di ricerca condotti in diverse tematiche. La tabella \
                evidenzia che l’ambito (32) Biomedical and Clinical Sciences, è quello con il maggior numero di progetti, con un totale di 187. Seguono Engineerings (40) \
                con 150 progetti e le Biological Sciences (31) con 143 progetti.')


st.markdown("### Somma finanziata per i 5 dipartimenti con più progetti nella Federico II")
fig = px.pie(dfs['Top5DipSommaProgetti'], values='Somma Finanziata (EUR)', names='Dipartimento')
fig.update_layout(margin=dict(t=0, b=0, l=0, r=0), yaxis={'categoryorder':'total ascending'})

st.plotly_chart(fig, use_container_width=True)
st.markdown('Nella Figura, vengono mostrate le somme finanziate tra i cinque dipartimenti universitari della Federico II con più progetti. La Figura evidenzia una \
            notevole differenza tra i finanziamenti ricevuti dai vari dipartimenti. Il Dipartimento di Ingegneria (B12 Engineering) risulta essere il più finanziato \
            con oltre un miliardo di euro, seguito dal Dipartimento di Informatica e Scienze dell’Informazione (B11 Computer Science and Informatics) con quasi mezzo \
            miliardo di euro. Gli altri tre dipartimenti mostrano somme di finanziamenti nettamente inferiori. Si può perfettamente dedurre come la Federico II sia \
            molto afferente ai dipartimenti di Ingegneria, anche perchè la stessa sede presenta un numero di facoltà di Ingegneria differenti molto alto.')


#lcol4, rcol4 = st.columns([6.4, 3.6])
#with lcol4:
st.markdown("### Gli ambiti di ricerca con progetti di maggiore durata")
st.dataframe(dfs['ricercadurata'], use_container_width=True)
st.markdown('Dalla Tabella si può dedurre che il dipartimento di Medicina Clinica (A01) è quello che intraprende progetti con la maggior durata di \
            ricerca. Infatti, nei primi 10 posti della classifica lo ritroviamo 3 volte con ben 2 progetti con una durata di 16 anni. Altri dipartimenti \
            con progetti molto lunghi nel tempo sono quelli di Professioni sanitarie alleate, Odontoiatria, Infermieristica e Farmacia (A03), Scienze \
            biologiche (A05) e Sistemi della Terra e Scienze Ambientali (B07), con progetti della durata rispettivamente di 15,14 e 11 anni. Dalla \
            precedente analisi, quindi, si può notare che l’Università ”Federico II” sia molto impegnata nei settori legati all’healthcare nonché a \
            quelli riguardanti rischi ambientali (es. terremoti) essendo legata ad un territorio come Napoli ad elevato rischio sismico.')


#with rcol4:
st.markdown("### Somma finanziata e numero di progetti della Federico II nel 2020")
st.dataframe(dfs['ProgFondi2020'], use_container_width=True)
st.markdown('Dai risultati si può notare che durante l’anno 2020 l’Università ha avviato il maggior numero di progetti nel settore medico, vedendo rispettivamente \
            ”Biomedical and Clinical Sciences”, ”Biological Sciences” e ”Oncology and Carcinogenesis” occupare le prime 3 posizioni per numero di progetti. Segue \
            poi ”Engineering”, che oltre alla presenza di 16 progetti è anche il campo che ha ricevuto la maggiore somma in termini di finanziamenti. Da questo punto \
            di vista, le tematiche come ”Information and Computing Sciences” e divesi settori dell’Ingegneria occupano i primi posti, nonostante un più basso numero \
            di progetti.')



st.markdown("### Numero di progetti e Somma Finanziata nei settori medici di ricerca")
st.dataframe(dfs['Settori_Medici'], use_container_width=True)
st.markdown('Dalla Tabella si può notare che la Federico II ha lavorato su molti progetti di ricerca e sviluppo nei settori medici, in particolare in Genetics, \
            Biotechnology e Clinical Research. La somma finanziata per i vari progetti varia notevolmente, ma questo potrebbe essere dovuto alla complessità e alla \
            durata dei progetti stessi. In generale, si può dire che la Federico II si concentra sulla ricerca e lo sviluppo di nuove terapie e tecnologie per \
            migliorare la salute umana.')


st.markdown("### I 10 Paesi esteri con cui ha maggiormente collaborato l’Università")
fig = px.choropleth(dfs['top10paesi'], locations=dfs['top10paesi'].Paese,
                    color=dfs['top10paesi'].Numero_Collaborazioni, # lifeExp is a column of gapminder
                    hover_name=dfs['top10paesi'].Paese, # column to add to hover information
                    color_continuous_scale=px.colors.sequential.Plasma,
                    width=800,
                    height=800)
fig.update_layout(yaxis={'categoryorder':'total ascending'})

st.plotly_chart(fig, use_container_width=True)
st.markdown('Come è possibile notare dalla Mappa, si evince che l’Università ”Federico II” ha collaborato principalmente con istituzioni del Regno Unito. In particolare, \
            sono stati effettuati ben 414 progetti con tale paese. A seguire, si trovano la Germania, la Francia e la Spagna con rispettivamente 385, 332 e 262 progetti, \
            da cui si può dedurre l’importanza dell’Università, non solo a livello nazionale ma anche internazionale nella ricerca. Inoltre, sempre dalla Mappa, si nota \
            che l’Università non collabora solo con paesi europei ma anche con paesi di altri continenti come gli Stati Uniti con cui ha collaborato per ben 135 progetti.')



lcol6, rcol6 = st.columns(2)
with lcol6:
    st.markdown("### Somma finanziata dalla Federico II nelle ricerche negli ultimi 20 anni")
    fig = px.bar(dfs['SommaFinanziata'], x='Start_Year', y='Somma_Finanziata_EUR')
    fig.update_layout(yaxis={'categoryorder':'total ascending'})
    
    st.plotly_chart(fig, use_container_width=True)
    st.markdown('In Figura vengono presentati i dati relativi ai finanziamenti ricevuti per la ricerca nel corso degli ultimi 20 anni. Si nota come i finanziamenti per la \
                ricerca hanno subito variazioni significative nel corso degli anni. L’anno con il più alto importo di finanziamenti è il 2020, con oltre 412 milioni di \
                euro, seguito dal 2022 con oltre 314 milioni di euro e dal 2014 con oltre 327 milioni di euro. Si può notare come dall’avvento dell’Euro la somma media annuale \
                finanziata sui progetti dalla Federico II sono stati più bassi rispetto alla media annuale finanziata negli ultimi 10 anni. Uno dei possibili motivi può \
                essere dovuta dalla evoluzione tecnologica e alla strumentazione all’avanguardia che negli ultimi anni richiedono una richiesta di denaro molto più alta oltre \
                anche all’aumento del valore dell’euro negli ultimi 10 anni.')


with rcol6:
    st.markdown("### Somma finanziata dalla Federico II nelle ricerche sul cancro negli ultimi 20 anni")
    fig = px.bar(dfs['SommaFinanziataCancro'], x='Start_Year', y='Somma_Finanziata_Cancro_EUR')
    fig.update_layout(yaxis={'categoryorder':'total ascending'})

    st.plotly_chart(fig, use_container_width=True)
    st.markdown('Negli ultimi 20 anni la Federico II ha costantemente finanziato progetti relativi alla ricerca sul cancro e continua a farlo tuttora. Con \
                i dati a disposizione, si può notare che negli anni più recenti, in particolare il 2017 e il 2020, sono state investite somme superiori ai 15 milioni, \
                mentre nel 2007 è stata anche superata la soglia dei 20 milioni. Si noti come, negli anni tra il 2008 ed il 2014 i fondi stanziati per i progetti sulla \
                ricerca abbiano risentito di un drastico decremento riconducibile in parte alla concomitante crisi finanziaria attraversata dal paese in quegli stessi \
                anni. Si noti inoltre come la Federico II abbia investito in media molto dal 2002 al 2007 in rapporto con gli investimenti totali nei rispettivi anni, \
                a dimostrazione del fatto che in quegli anni l’Università era intenta a consolidarsi tra le migliori Università nella ricerca di soluzioni imminenti al Cancro.')


# ---- HIDE STREAMLIT STYLE ----
hide_st_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            header {visibility: hidden;}
            </style>
            """
st.markdown(hide_st_style, unsafe_allow_html=True)
