import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import re
import pyspark.sql.functions as f

conf = SparkConf().setAppName('T3_Time')
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

# formato de la fecha -> 2016-02-09 06:46:32
# me quedo con la hora 
df = df.withColumn("Time", f.substring(df["Start_Time"], 12, 2))
df = df.withColumn("Time", df["Time"].cast("int"))

#
df = df.withColumn("Time_Range", f.when((df["Time"] >= 0) & (df["Time"] < 6), "00:00 - 05:59")
                                 .when((df["Time"] >= 6) & (df["Time"] < 12), "06:00 - 11:59")
                                 .when((df["Time"] >= 12) & (df["Time"] < 18), "12:00 - 17:59")
                                 .otherwise("18:00 - 23:59"))

# agrupamos por dia se la semana y contamos el total de accidentes
df1 = df.groupBy(df["Time_Range"]).count()

# hacemos la media de la severidad
df2 = df.withColumn("Severity", df["Severity"].cast("int"))
df2 = df2.groupBy("Time_Range").mean("Severity")

# juntamos las tablas
dfFinal = df1.join(df2, on=["Time_Range"])
dfFinal = dfFinal.sort("Time_Range")

# guardar el resultado en un fichero
dfFinal.toPandas().to_excel("output_T3_Time.xlsx", index=False)

# sacamos el resultado x pantalla
dfFinal.show()

