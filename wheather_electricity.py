import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, round as spark_round, corr
import plotly.express as px
from df import groupage, graphique, filtre, df, correlation

st.sidebar.info("📊 Explorez l'impact des variables météorologiques sur la consommation d'électricité.")
df = df
variables = [
 'Pression au niveau mer',
 'Variation de pression en 3 heures',
 'Vitesse du vent moyen 10 mn',
 'Humidité',
 'Rafales sur une période',
 'Précipitations dans les 3 dernières heures',
 'Température (°C)',
 'consommation'
 ]
selected_agregation = st.sidebar.selectbox("Quelle niveau d'agrégation ?", ["Journalier", "Mensuel"])
if selected_agregation == "Journalier":
    df_agreg = groupage(df,"day")
elif selected_agregation == "Mensuel":
    df_agreg = groupage(df,"month")
else:
    df_agreg = df


st.sidebar.title("🔍 Sélection de la période")
selected_year = st.sidebar.selectbox("Sélectionnez une année", ["Toutes",2013, 2014,2015,2016,2017,2018,2019,2020,2021,2022])


selected_variable = st.sidebar.selectbox("Sélectionner une variable",variables) 
if selected_year != "Toutes":
    df_agreg = filtre(df_agreg,"Annee",selected_year)


def display_metrics(df_agreg):
    metrics = df_agreg.select(
        spark_round(avg("consommation"), 2).alias("Moyenne"),
        spark_round(max("consommation"), 2).alias("Max"),
        spark_round(min("consommation"), 2).alias("Min")
    ).collect()[0]
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Moyenne consommation", metrics["Moyenne"])
    col2.metric("Max consommation", metrics["Max"])
    col3.metric("Min consommation", metrics["Min"])

# Affichage des graphiques
st.title("⚡ Impact de la Météo sur la Consommation d'Électricité")

display_metrics(df_agreg)

# Graphique consommation et température
df_pandas = df_agreg.toPandas()
fig = px.line(df_pandas, x="Date_Heure", y=selected_variable, title=selected_variable)
st.plotly_chart(fig)

# Graphique vent et consommation
fig2 = px.scatter(df_pandas, x=selected_variable, y="consommation", title=f"Corrélation entre {selected_variable} et la Consommation")
st.plotly_chart(fig2)

# Afficher un DataFrame avec la corrélation
df_correlation = df_agreg.select(corr("consommation", selected_variable).alias(f"Corrélation Consommation-{selected_variable}"))
st.write("Résultat de la corrélation :")
st.dataframe(df_correlation.toPandas())



