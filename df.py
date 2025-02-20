from pyspark.sql import SparkSession
from pyspark.sql.functions import avg,desc, sum,max,corr,date_trunc, col, year, month, dayofmonth, hour, minute, second, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType
import matplotlib.pyplot as plt
import pandas as pd
import setuptools
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import os
#os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"

# Initialisation de la session Spark
spark_session = SparkSession.builder.appName("Analyse").getOrCreate()
df = spark_session.read.csv('./data/donnees.csv',header = True, inferSchema=True)


# Agrégation des données horaires en données journalières en utilisant la moyenne
def groupage(base, temps : str):
    base_agregee = base.groupBy(date_trunc(temps, col("Date_Heure")).alias("Date_Heure")).agg(
        avg("Pression au niveau mer").alias("Pression au niveau mer"),
        avg("Variation de pression en 3 heures").alias("Variation de pression en 3 heures"),
        avg("Type de tendance barométrique").alias("Type de tendance barométrique"),
        avg("Direction du vent moyen 10 mn").alias("Direction du vent moyen 10 mn"),
        avg("Vitesse du vent moyen 10 mn").alias("Vitesse du vent moyen 10 mn"),
        avg("Température").alias("Température"),
        avg("Point de rosée").alias("Point de rosée"),
        avg("Humidité").alias("Humidité"),
        avg("Visibilité horizontale").alias("Visibilité horizontale"),
        avg("Temps présent").alias("Temps présent"),
        avg("Pression station").alias("Pression station"),
        max("Rafales sur une période").alias("Rafales sur une période"),
        avg("Periode de mesure de la rafale").alias("Periode de mesure de la rafale"),
        avg("Précipitations dans la dernière heure").alias("Précipitations dans la dernière heure"),
        sum("Précipitations dans les 3 dernières heures").alias("Précipitations dans les 3 dernières heures"),
        avg("Température (°C)").alias("Température (°C)"),
        avg("consommation").alias("consommation")
    )
    base_agregee = base_agregee.withColumn("Jour", dayofmonth(col("Date_Heure"))) \
       .withColumn("Mois", month(col("Date_Heure"))) \
       .withColumn("Annee", year(col("Date_Heure")))
    
    # Ajout d'un nouvel index pour chaque ligne
    window_spec = Window.orderBy("Date_Heure")
    base_agregee = base_agregee.withColumn("index", F.row_number().over(window_spec))
    # Réorganiser les colonnes pour que 'index' soit la première colonne
    base_agregee = base_agregee.select("index", *[col for col in base_agregee.columns if col != "index"])
    return base_agregee



#Fonction servant à filtrer les données afin de pour faire des représentations en fonction des périodes.
def filtre(base, variable, valeur):
    filtered_df = base.filter(base[variable] == valeur)
    return filtered_df


# Conversion en DataFrame Pandas
def graphique(base, variable):
    pandas_df = base.toPandas()

    plt.figure(figsize=(10, 6))
    plt.plot(pandas_df['Date_Heure'], pandas_df[variable])
    #plt.title(f'Température en 2015')
    plt.xlabel('Date et Heure')
    plt.ylabel(f'{variable}')
    plt.show()

def correlation(var2):
    coef = corr("consommation", var2)
    df_correlation = df.select(
        coef.alias(f"Corrélation Consommation d'électricité-{var2}"))
    return df_correlation

