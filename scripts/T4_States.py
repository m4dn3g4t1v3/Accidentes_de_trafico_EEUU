import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import re
import pyspark.sql.functions as f
import matplotlib.pyplot as plt
import pandas

conf = SparkConf().setAppName('T4_States')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

path = "US_Accidents_Dec20_updated.csv"
df = spark.read.option("header", "true").csv(path)

# limpiar df de columnas no necesarias para quedarme con Severity, State
df = df.drop("ID", "Start_Time", "End_Time","Start_Lat","Start_Lng","End_Lat","End_Lng",
"Distance(mi)","Description","Number","Street", "Side","City","County",
"Zipcode","Country","Timezone","Airport_Code","Weather_Timestamp",
"Temperature(F)","Wind_Chill(F)","Humidity(%)","Pressure(in)","Visibility(mi)",
"Wind_Direction","Wind_Speed(mph)","Precipitation(in)","Weather_Condition","Amenity",
"Bump","Crossing","Give_Way","Junction","No_Exit","Railway","Roundabout","Station",
"Stop","Traffic_Calming","Traffic_Signal","Turning_Loop","Sunrise_Sunset","Civil_Twilight",
"Nautical_Twilight","Astronomical_Twilight")

# creo los df de numero de accidentes por estado
df1 = df.groupBy(df["State"]).count()

# hacemos la media de la severidad
df2 = df.withColumn("Severity", df["Severity"].cast("int"))
df2 = df2.groupBy("State").mean("Severity")

# juntamos las tablas
dfFinal = df1.join(df2, on=["State"])
dfFinal = dfFinal.sort("State")

# guardar el resultado en un fichero
dfFinal.toPandas().to_excel("output_T4_States.xlsx", index=False)

# sacamos el resultado x pantalla
dfFinal.show(dfFinal.count(), False)

