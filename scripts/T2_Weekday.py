import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import re
import pyspark.sql.functions as f

conf = SparkConf().setAppName('T2_Weekday')
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
# me quedo con el dia, mes y año nada mas en la fecha
df = df.withColumn("Start_Time", f.substring_index(df["Start_Time"], " ", 1))

# añado otra columna con el día se la semana correspondiente
df = df.withColumn("Weekday", f.date_format(df["Start_Time"], "EEEE"))

# agrupamos por dia se la semana y contamos el total de accidentes
df1 = df.groupBy(df["Weekday"]).count()

# hacemos la media de la severidad
df2 = df.withColumn("Severity", df["Severity"].cast("int"))
df2 = df2.groupBy("Weekday").mean("Severity")

# juntamos las tablas
dfFinal = df1.join(df2, on=["Weekday"])
dfFinal = dfFinal.sort("avg(Severity)", ascending=False)

# guardar el resultado en un fichero
dfFinal.toPandas().to_excel("output_T2_Weekday.xlsx", index=False)

# sacamos el resultado x pantalla
dfFinal.show(dfFinal.count(), False)

