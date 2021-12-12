import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import re
import pyspark.sql.functions as f
import pandas as pd
import matplotlib as plt

conf = SparkConf().setAppName('T1_Months')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

path = "US_Accidents_Dec20_updated.csv"
df = spark.read.option("header", "true").csv(path)

# limpiar df de columnas no necesarias para quedarme con Severity, Start_Time
df = df.drop("ID", "End_Time","Start_Lat","Start_Lng","End_Lat","End_Lng",
"Distance(mi)","Description","Number","Street", "Side","City","County","State",
"Zipcode","Country","Timezone","Airport_Code","Weather_Timestamp",
"Temperature(F)","Wind_Chill(F)","Humidity(%)","Pressure(in)","Visibility(mi)",
"Wind_Direction","Wind_Speed(mph)","Precipitation(in)","Weather_Condition","Amenity",
"Bump","Crossing","Give_Way","Junction","No_Exit","Railway","Roundabout","Station",
"Stop","Traffic_Calming","Traffic_Signal","Turning_Loop","Sunrise_Sunset","Civil_Twilight",
"Nautical_Twilight","Astronomical_Twilight")

# me quedo con el mes y el año nada mas en la fecha
df = df.withColumn("Start_Time", f.substring_index(df["Start_Time"], "-", 2))

# casteo a int la severidad para tratarla
df = df.withColumn("Severity", df["Severity"].cast("int"))

# creo los 4 dfs de numero de accidentes por mes correspondiente a cada nivel de severidad
severidad1 = df.filter(f.col('Severity') == 1).groupBy("Start_Time").count().withColumnRenamed("count","Severity 1")
severidad2 = df.filter(f.col('Severity') == 2).groupBy("Start_Time").count().withColumnRenamed("count","Severity 2")
severidad3 = df.filter(f.col('Severity') == 3).groupBy("Start_Time").count().withColumnRenamed("count","Severity 3")
severidad4 = df.filter(f.col('Severity') == 4).groupBy("Start_Time").count().withColumnRenamed("count","Severity 4")

# ordenamos estos dfs por fecha
severidad1 = severidad1.sort(severidad1["Start_Time"])
severidad2 = severidad2.sort(severidad2["Start_Time"])
severidad3 = severidad3.sort(severidad3["Start_Time"])
severidad4 = severidad4.sort(severidad4["Start_Time"])

#juntar todas en una df
dfFinal = severidad1.join(severidad2, on=["Start_Time"], how ='full')
dfFinal = dfFinal.join(severidad3, on=["Start_Time"], how ='full')
dfFinal = dfFinal.join(severidad4, on=["Start_Time"], how ='full')
dfFinal = dfFinal.sort(dfFinal["Start_Time"])
dfFinal = dfFinal.fillna(value=0)

# guardar el resultado en un fichero
dfFinal.toPandas().to_excel("output_T1_Months.xlsx", index=False)

# sacamos el resultado x pantalla
dfFinal.show(dfFinal.count(), False)

