import json
import streamlit as st
from datetime import datetime
import plotly.express as px
from pyspark.sql.functions import asc, explode, avg, col, substring, monotonically_increasing_id


# Ottiene la data e ora corrente
current_time = datetime.now().strftime("%Y-%m-%dT%H:%M")


# Configura la pagina Streamlit
st.set_page_config('MeteoK', ':mostly_sunny:', layout='wide')
st.title(':mostly_sunny: MeteoK')
st.header("Analytics meteorologiche")


# Crea lo Spark Context e la Session
def openSpark(citta_scelta):
    import findspark
    findspark.init()

    from pyspark.context import SparkContext
    sc = SparkContext.getOrCreate()

    from pyspark.sql import SparkSession
    spark = SparkSession(sc)

    # Legge le info meteo relative alla città selezionata
    info_meteo = spark.read \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .json(f'hdfs://localhost:9000/giuseppericcio/bigdata/{citta_scelta}.json')

    return info_meteo


# ----------------------- SIDEBAR ------------------------------------
# Seleziona la città
with open('capoluoghi.json') as file:
    capoluoghi = json.load(file)

nomi_citta = []
for capoluogo in capoluoghi['capoluoghi']:
    nomi_citta.append(capoluogo['nome'])

citta_scelta = st.sidebar.selectbox('Scegli città', options=nomi_citta)
info_meteo = openSpark(citta_scelta)

# Ottiene la emoji relativa al Weather code nel JSON
def get_weather_info(weather_value):
    if weather_value == 0:
        emoji = "☀️"
        title = "Cielo sereno"
    elif 3 <= weather_value <= 45:
        emoji = "⛅️"
        title = "Parzialmente nuvoloso"
    elif 45 <= weather_value <= 50:
        emoji = "🌫️"
        title = "Nebbia e nuvoloso"
    elif 50 <= weather_value <= 70:
        emoji = "🌧️"
        title = "Pioggia"
    elif 70 <= weather_value <= 85:
        emoji = "⛈️"
        title = "Pioggia forte"
    elif 85 <= weather_value <= 90:
        emoji = "🌨️"
        title = "Nevicata"
    elif 90 <= weather_value <= 100:
        emoji = "🌩️"
        title = "Temporale"
    else:
        emoji = "❓"
        title = "Condizioni meteorologiche non definite"

    return emoji, title

# Mostra l'emoji ed il titolo del meteo corrente
weather_curr_value = info_meteo.select('current_weather.weathercode').first()[0]
weather_emoji, weather_title = get_weather_info(weather_curr_value)

st.sidebar.markdown("<span style='font-size:45px;'>{} **{}**</span>"
                    .format(weather_emoji, citta_scelta), unsafe_allow_html=True)
st.sidebar.subheader(f"{weather_title}")

# Mostra l'ultimo aggiornamento del meteo
data_curr_value = info_meteo.select('current_weather.time').first()[0]
st.sidebar.caption(f"Ultimo aggiornamento: {data_curr_value}")

# Mostra la temperatura corrente
temp_curr_value = info_meteo.select('current_weather.temperature').first()[0]
st.sidebar.metric(label=":thermometer: Temperatura adesso",
                  value=f"{temp_curr_value}°C", delta="")

# Ottiene le emoji del vento corrente
def get_wind_info(wind_value):
    if wind_value == 0:
        emoji = "🌬️"
        title = "Assenza di vento"
    elif 0 < wind_value <= 45:
        emoji = "🌬️"
        title = "Vento moderato"
    elif 45 < wind_value <= 90:
        emoji = "🌀"
        title = "Vento forte"
    elif 90 < wind_value <= 135:
        emoji = "🌪️"
        title = "Vento molto forte"
    else:
        emoji = "🌬️"
        title = "Condizioni di vento estreme"

    return emoji, title

# Ottiene le info sul tipo di vento
def get_wind_type(wind_direction):
    if 337.5 <= wind_direction <= 22.5:
        return "Tramontana - N"
    elif 22.5 < wind_direction <= 67.5:
        return "Grecale - NE"
    elif 67.5 < wind_direction <= 112.5:
        return "Levante - E"
    elif 112.5 < wind_direction <= 157.5:
        return "Sirocco - SE"
    elif 157.5 < wind_direction <= 202.5:
        return "Ostro - S"
    elif 202.5 < wind_direction <= 247.5:
        return "Libeccio - SW"
    elif 247.5 < wind_direction <= 292.5:
        return "Ponente - W"
    elif 292.5 < wind_direction <= 337.5:
        return "Maestrale - NW"
    else:
        return "Unknown"

#Mostra l'emoji ed il titolo del vento corrente
vento_curr_value = info_meteo.select('current_weather.windspeed').first()[0]
wind_emoji, wind_title = get_wind_info(vento_curr_value)

dir_vento_curr_value = info_meteo.select('current_weather.winddirection').first()[0]
wind_type = get_wind_type(dir_vento_curr_value)

# Crea due colonne
col1, col2 = st.sidebar.columns(2)
# Metrica 1 nella prima colonna
with col1:
    st.metric(label="Velocità Vento", value=f"{vento_curr_value}Km/h", delta_color="off", delta=wind_title)
# Metrica 2 nella seconda colonna
with col2:
    st.metric(label="Direzione Vento", value=f"{dir_vento_curr_value}°", delta_color="off", delta=wind_type)


## ----------------------- BODY ---------------------------------------
# ANALYTIC 1
# Esegue la query per ottenere l'andamento della temperatura nel tempo
result_df = info_meteo.select(
    'hourly.time', 'hourly.temperature_2m', 'hourly.apparent_temperature')

# Converte il risultato in un DataFrame pandas
result_pd = result_df.toPandas()

# Visualizza il line chart utilizzando Plotly Express
fig = px.line(result_pd, x=result_pd['time'][0], y=[result_pd['temperature_2m'][0],result_pd['apparent_temperature'][0]], 
              title='Andamento della temperatura nel tempo')

# Aggiunge le etichette alle linee plottate
newnames = {'wide_variable_0':'Temperatura', 'wide_variable_1': 'Temperatura Apparente'}
fig.for_each_trace(lambda t: t.update(name = newnames[t.name],
                             legendgroup = newnames[t.name],
                             hovertemplate='%{y:.1f} °C',
                             line=dict(width=2.5)
                             )
                  )

# Aggiunge la legenda
fig.update_layout(
    legend=dict(
        title='Legenda',
        title_font=dict(size=14),
        orientation='h',
        yanchor='bottom',
        y=1.02,
        xanchor='right',
        x=1
    )
)

# Aggiunge le label alle variabili x ed y
fig.update_yaxes(title_text='Temperatura (°C)', tickformat=".1f")
fig.update_xaxes(title_text='Data e Ora')

# Visualizza il line chart su Streamlit
st.plotly_chart(fig, use_container_width=True)


# ANALYTIC 2
# Esegue la query per ottenere l'andamento dell'umidità nel tempo
result_df = info_meteo.select('hourly.time', 'hourly.relativehumidity_2m')

# Converte il risultato in un DataFrame pandas
result_pd = result_df.toPandas()

# Visualizza il line chart utilizzando Plotly Express
fig = px.line(result_pd, x=result_pd['time'][0], y=result_pd['relativehumidity_2m'][0], 
              title='Andamento dell`umidità nel tempo')

# Aggiunge l'etichetta alla linea plottata
fig.update_traces(
    name = 'Umidità',
    hovertemplate='Data: %{x}<br> Umidità: %{y} %',
    line=dict(width=2.5)
)

# Aggiunge le label alle variabili x e y
fig.update_yaxes(title_text='Umidità (%)', tickformat=".1f")
fig.update_xaxes(title_text='Data e Ora')

# Visualizza il line chart su Streamlit
st.plotly_chart(fig, use_container_width=True)


# ANALYTIC 3
# Esegue la query per ottenere l'andamento della velocità del vento nel tempo
result_df = info_meteo.select('hourly.time', 'hourly.windspeed_10m')

# Converte il risultato in un DataFrame pandas
result_pd = result_df.toPandas()

# Visualizza il line chart utilizzando Plotly Express
fig = px.line(result_pd, x=result_pd['time'][0], y=result_pd['windspeed_10m'][0], 
              title='Andamento della velocità del vento nel tempo')

# Aggiunge l'etichetta alla linea plottata
fig.update_traces(
    name = 'Velocità',
    hovertemplate='Data: %{x}<br> Velocità: %{y} km/h',
    line=dict(width=2.5)
)

# Aggiunge le label alle variabili x e y
fig.update_yaxes(title_text='Velocità del vento (km/h)', tickformat=".1f")
fig.update_xaxes(title_text='Data e Ora')

# Visualizza il line chart su Streamlit
st.plotly_chart(fig, use_container_width=True)


# ANALYTIC 4
# Seleziona le colonne "time" e "temperature"
data = info_meteo.select(explode("hourly.time").alias("time"))
temp = info_meteo.select(explode("hourly.temperature_2m").alias("temp"))

# Estrarre la data dalla colonna "time", escludendo l'ora
data_formattata = data.withColumn("date", substring(col("time"), 1, 10)) \
                      .select("date")

# Aggiunge un indice ai 2 DataFrame
data_formattata_with_index = data_formattata.withColumn("index", monotonically_increasing_id())
temp_with_index = temp.withColumn("index", monotonically_increasing_id())

# Esegue la join basata sull'indice
joined_df = data_formattata_with_index.join(temp_with_index, on=["index"], how="inner")

# Seleziona solo le colonne desiderate
result_df = joined_df.select("date", "temp")

# Calcola la temperatura media giornaliera
daily_avg_temp = result_df.groupBy("date") \
                          .agg(avg("temp").alias("avg_temp")) \
                          .sort(asc("date"))

# Converte il risultato in un DataFrame pandas
result_pd = daily_avg_temp.toPandas()

# Visualizza il line chart utilizzando Plotly Express
fig = px.line(result_pd, x="date", y="avg_temp",
              title="Temperatura media giornaliera")

# Aggiunge l'etichetta alla linea plottata
fig.update_traces(
    name = 'Temperatura media',
    hovertemplate='Data: %{x}<br> Temperatura media: %{y} °C',
    line=dict(width=2.5)
)

# Aggiunge le label alle variabili x e y
fig.update_yaxes(title_text='Temperatura media (°C)', tickformat=".1f")
fig.update_xaxes(title_text='Giorno')

# Visualizza il line chart su Streamlit
st.plotly_chart(fig, use_container_width=True)


# ANALYTIC 5
# Seleziona le colonne "time" e "precipitation"
data = info_meteo.select(explode("hourly.time").alias("time"))
prec = info_meteo.select(explode("hourly.precipitation").alias("precipitation"))

# Aggiunge un indice ai 2 DataFrame
data_with_index = data.withColumn("index", monotonically_increasing_id())
prec_with_index = prec.withColumn("index", monotonically_increasing_id())

# Esegue la join basata sull'indice
joined_df = data_with_index.join(prec_with_index, on=["index"], how="inner")

# Seleziona solo le colonne desiderate
result_df = joined_df.select("time", "precipitation")

# Esegue la query per ottenere l'andamento delle precipitazioni nei giorni precedenti
filtered_df = result_df.select('time', 'precipitation') \
                       .filter(col('time') <= current_time)

# Converte il risultato in un DataFrame pandas
result_pd = filtered_df.toPandas()

# Visualizza il bar chart utilizzando Plotly Express
fig = px.bar(result_pd, x="time", y="precipitation", 
             title='Andamento delle precipitazioni nei giorni precedenti')

# Aggiunge l'etichetta alla linea plottata
fig.update_traces(
    name = 'Precipitazioni',
    hovertemplate='Data: %{x}<br> Precipitazioni: %{y} mm'
)

# Aggiunge le label alle variabili x e y
fig.update_yaxes(title_text='Precipitazioni (mm)', tickformat=".1f")
fig.update_xaxes(title_text='Data e Ora')

# Visualizza il bar chart su Streamlit
st.plotly_chart(fig, use_container_width=True)


# ANALYTIC 5.5
# Seleziona le colonne "time" e "precipitation_probability"
data = info_meteo.select(explode("hourly.time").alias("time"))
prec_prob = info_meteo.select(explode("hourly.precipitation_probability").alias("precipitation_probability"))

# Aggiunge un indice ai 2 DataFrame
data_with_index = data.withColumn("index", monotonically_increasing_id())
prec_prob_with_index = prec_prob.withColumn("index", monotonically_increasing_id())

# Esegue la join basata sull'indice
joined_df = data_with_index.join(prec_prob_with_index, on=["index"], how="inner")

# Seleziona solo le colonne desiderate
result_df = joined_df.select("time", "precipitation_probability")

# Esegue la query per ottenere l'andamento della probabilità di precipitazioni nei prossimi giorni
filtered_df = result_df.select('time', 'precipitation_probability') \
                       .filter(col('time') > current_time)

# Converte il risultato in un DataFrame pandas
result_pd = filtered_df.toPandas()

# Visualizza il bar chart utilizzando Plotly Express
fig = px.bar(result_pd, x="time", y="precipitation_probability", 
             title='Andamento della probabilità di precipitazioni nel tempo')

# Aggiunge l'etichetta alla linea plottata
fig.update_traces(
    name = 'Probabilità di Precipitazioni',
    hovertemplate='Data: %{x}<br> Probabilità di Precipitazioni: %{y} %'
)

# Aggiunge le label alle variabili x e y
fig.update_yaxes(title_text='Probabilità di Precipitazioni (%)', tickformat=".1f")
fig.update_xaxes(title_text='Data e Ora')

# Visualizza il bar chart su Streamlit
st.plotly_chart(fig, use_container_width=True)


# ANALYTIC 6
# Esegue la query per ottenere l'andamento della pressione nel tempo
result_df = info_meteo.select('hourly.time', 'hourly.pressure_msl')

# Converti il risultato in un DataFrame pandas
result_pd = result_df.toPandas()

# Visualizza il line chart utilizzando Plotly Express
fig = px.line(result_pd, x=result_pd['time'][0], y=result_pd['pressure_msl'][0], 
              title='Andamento della pressione nel tempo')

# Aggiunge l'etichetta alla linea plottata
fig.update_traces(
    name = 'Pressione',
    hovertemplate='Data: %{x}<br> Pressione: %{y} hPa',
    line=dict(width=2.5)
)

# Aggiunge le label alle variabili x e y
fig.update_yaxes(title_text='Pressione (hPa)', tickformat=".1f")
fig.update_xaxes(title_text='Data e Ora')

# Visualizza il line chart su Streamlit
st.plotly_chart(fig, use_container_width=True)