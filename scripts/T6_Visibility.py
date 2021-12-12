from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
import re
import pyspark.sql.functions as f


conf = SparkConf().setAppName('T6_Visibility')
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

df = spark.read.option("header", "true").csv("US_Accidents_Dec20_updated.csv")

# limpiar df de columnas no necesarias para quedarme con Severity, Start_Time
df = df.drop("ID","Weather_Condition","Start_Time", "End_Time","Start_Lat","Start_Lng","End_Lat","End_Lng",
"Distance(mi)","Description","Number","Street", "Side","City","County","State",
"Zipcode","Country","Timezone","Airport_Code","Weather_Timestamp",
"Temperature(F)","Wind_Chill(F)","Humidity(%)","Pressure(in)",
"Wind_Direction","Wind_Speed(mph)","Precipitation(in)","Amenity",
"Bump","Crossing","Give_Way","Junction","No_Exit","Railway","Roundabout","Station",
"Stop","Traffic_Calming","Traffic_Signal","Turning_Loop","Sunrise_Sunset","Civil_Twilight",
"Nautical_Twilight","Astronomical_Twilight")

# creo los df de numero de accidentes por visibilidad
df1 = df.groupBy("Visibility(mi)").count()

# hacemos la media de la severidad
df2 = df.withColumn("Severity", df["Severity"].cast("int"))
df2 = df2.groupBy("Visibility(mi)").mean("Severity")

# juntamos las tablas 
dfFinal = df1.join(df2, on=["Visibility(mi)"])
dfFinal = dfFinal.sort("Visibility(mi)",ascending=False)

# guardar el resultado en un fichero
dfFinal.toPandas().to_excel("output_T6_Visibility.xlsx", index=False)

# sacamos el resultado x pantalla
dfFinal.show(dfFinal.count(),False)
