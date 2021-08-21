
#########################################################
##### EJERCICIOS DE SPARK
##### ARTURO R. 21/08/2021
#########################################################

--- conexion hive
beeline -u jdbc:hive2://

-- Consola Hive : En HIVE, listamos las bases de datos existentes
SHOW DATABASES;

--Consola Hive : Creamos database en un ruta pre definida e incluimos propiedades
CREATE DATABASE IF NOT EXISTS TEMPORAL 
COMMENT 'Base de datos temporal'
LOCATION '/proyectos/temporal'
WITH DBPROPERTIES ('creator' = 'Arturo Rojas', 'date' = '2021-08-21');

--Consola Hive : 
-- En HIVE, creamos la tabla
CREATE EXTERNAL TABLE IF NOT EXISTS TEMPORAL.CLIENTE(
ID STRING,
NOMBRE STRING,
TELEFONO STRING,
CORREO STRING,
FECHA_INGRESO STRING,
EDAD INT,
SALARIO DOUBLE,
ID_EMPRESA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ('creator'='Arturo Rojas', 'created_at'='2020-04-18', 'skip.header.line.count'='1');

LOAD DATA LOCAL INPATH 'dataset/cliente.data' INTO TABLE TEMPORAL.CLIENTE;

--Consola Hive : 
-- Verificamos
SHOW TABLES IN TEMPORAL;


--Consola Hive : 
CREATE EXTERNAL TABLE IF NOT EXISTS TEMPORAL.TRANSACCION(
ID_PERSONA STRING,
ID_EMPRESA STRING,
MONTO DOUBLE,
FECHA STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ('creator'='Arturo Rojas', 'created_at'='2021-08-14 09:30:00', 'skip.header.line.count'='1');

--Consola HIVE : 
--CARGAMOS LA DATA DE TRANSACCIONES
LOAD DATA LOCAL INPATH 'dataset/transacciones.data' INTO TABLE TEMPORAL.TRANSACCION;



-----------------------

--Consola Hive : 
CREATE TABLE TEMPORAL.EMPRESA(
ID STRING,
NOMBRE STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ('creator'='Arturo Rojas', 'created_at'='2021-04-24', 'skip.header.line.count'='1');
--Consola Hive : 
LOAD DATA LOCAL INPATH 'dataset/empresa.data' INTO TABLE TEMPORAL.EMPRESA;

---------------------------

--Consola Hive : 
CREATE TABLE TEMPORAL.TABLON_TRANSACCIONES(
ID_CLIENTE STRING,
NOMBRE_PERSONA STRING,
EDAD_PERSONA INT,
SALARIO_PERSONA DOUBLE,
ID_EMPRESA STRING,
MONTO_TRANSACCION DOUBLE,
FECHA_TRANSACCION STRING
NOMBRE_EMPRESA STRING
)
STORED AS PARQUET;




####################################################


####################################################
# Visualizamos la data de clientes
####################################################

df1 = spark.sql('SELECT * FROM temporal.cliente limit 10')
#Mostramos el contenido de nuestra variable
df1.show()
df1.printSchema()

##########################################################################################################
# Visualizamos la data de clientes mayores a 25 años
##########################################################################################################

df2 = spark.sql('SELECT * FROM temporal.cliente where edad > 25')
#Mostramos el contenido de nuestra variable
df2.show()
df2.printSchema()

##########################################################################################################
# Visualizamos la cantidad de clientes 
##########################################################################################################

df3 = spark.sql('SELECT count(*) FROM temporal.cliente')
df3.show()


##########################################################################################################
# CREANDO DATAFRAME TEMPORAL CON LA INFORMACION DE CLIENTE Y TRANSACCION
##########################################################################################################


df4= spark.sql('SELECT T.ID_PERSONA, P.NOMBRE AS NOMBRE_PERSONA, CAST(P.EDAD AS INT) AS EDAD, CAST(P.SALARIO AS DOUBLE) AS SALARIO, T.ID_EMPRESA, CAST(T.MONTO AS DOUBLE) AS MONTO_TRANSACCION, T.FECHA FROM TEMPORAL.TRANSACCION T JOIN TEMPORAL.CLIENTE P ON T.ID_PERSONA = P.ID')
#Mostramos el contenido de nuestra variable
df4.show()
df4.createOrReplaceTempView("df4")

##########################################################################################################
# CREANDO DATAFRAME TEMPORAL CON LA INFORMACION DE EMPRESA Y EL RESULTADO DEL DATAFRAME ANTERIOR
##########################################################################################################

df5 = spark.sql('SELECT T.ID_PERSONA, T.NOMBRE_PERSONA, T.EDAD, T.SALARIO, E.NOMBRE AS NOMBRE_EMPRESA, T.MONTO_TRANSACCION, T.FECHA, T.ID_EMPRESA FROM df4 T JOIN TEMPORAL.EMPRESA E ON T.ID_EMPRESA = E.ID')
df5.show()
df5.createOrReplaceTempView("df5")

df5.printSchema()

##########################################################################################################
# CREANDO DATAFRAME TEMPORAL PARA INSERTAR LA INFORMACION EN LA TABLA TRANSACCIONES
##########################################################################################################


df6= spark.sql('INSERT INTO TEMPORAL.TABLON_TRANSACCIONES SELECT ID_PERSONA, NOMBRE_PERSONA, EDAD, SALARIO, ID_EMPRESA, NOMBRE_EMPRESA, MONTO_TRANSACCION, FECHA FROM df5 ')

## verificamos:
df7 = spark.sql('select * from TEMPORAL.TABLON_TRANSACCIONES')
df7.show(20)


##########################################################################################################
# PROCEDIMIENTO PARA LECTURA DE ARCHIVOS PLANOS EN HDFS
##########################################################################################################

dfDataHdfs = spark.read.format("csv").option("header", "true").option("delimiter", "|").load("hdfs:///proyectos/temporal/cliente/cliente.data")

#Mostramos la data
dfDataHdfs.show()

#Mostramos el esquema de la data
dfDataHdfs.printSchema()   




##########################################################################################################
# Librerías
##########################################################################################################


from pyspark.sql.types import StructType, StructField

#Importamos los tipos de datos que utilizaremos
from pyspark.sql.types import StringType, IntegerType, DoubleType

#Esta librería tiene otros tipos de datos
from pyspark.sql.types import *

#Leemos el archivo indicando el esquema definido
dfData = spark.read.format("csv").option("header", "true").option("delimiter", "|").schema(
    StructType(
        [
            StructField("ID", StringType(), True),
            StructField("NOMBRE", StringType(), True),
            StructField("TELEFONO", StringType(), True),
            StructField("CORREO", StringType(), True),
            StructField("FECHA_INGRESO", StringType(), True),
            StructField("EDAD", IntegerType(), True),
            StructField("SALARIO", DoubleType(), True),
            StructField("ID_EMPRESA", StringType(), True)
        ]
    )
).load("hdfs:///proyectos/temporal/cliente/cliente.data")

#Mostramos la data
dfData.show(10)

#Mostramos el esquema de la data
dfData.printSchema()




##########################################################################################################
#
# Procesos de Transformation
#
##########################################################################################################


#SELECT ID, NOMBRE, EDAD FROM dfData
#Seleccionamos algunas columnas
df1 = dfData.select(dfData["ID"], dfData["NOMBRE"], dfData["EDAD"])

#Mostramos los datos
df1.show()

##########################################################################################################
#
# Procesos de Transformation con FILTER
#
##########################################################################################################


#SELECT * FROM dfData WHERE EDAD > 60

#Hacemos un filtro
df2 = dfData.filter(dfData["EDAD"] > 60)

#Mostramos los datos
df2.show()

#Hacemos un filtro con un "and"
#SELECT * FROM dfData WHERE EDAD > 60 AND SALARIO > 20000
df3 = dfData.filter((dfData["EDAD"] > 60) & (dfData["SALARIO"] > 20000))

#Mostramos los datos
df3.show()

#Hacemos un filtro con un "or"
#SELECT * FROM dfData WHERE EDAD > 60 OR SALARIO < 20
df4 = dfData.filter((dfData["EDAD"] > 60) | (dfData["SALARIO"] < 2000))

#Mostramos los datos
df4.show()


##########################################################################################################
#
# GROUP BY
#
##########################################################################################################

#OPERACION EQUIVALENTE EN SQL:
#SELECT 
#	EDAD
#	COUNT(EDAD)
#	MIN(FECHA_INGRESO)
#	SUM(SALARIO)
#	MAX(SALARIO)
#FROM
#	dfData
#GROUP BY
#	EDAD

#Importamos la librería de funciones
import pyspark.sql.functions as f

#GROUP BY
df5 = dfData.groupBy(dfData["EDAD"]).agg(
	f.count(dfData["EDAD"]), 
	f.min(dfData["FECHA_INGRESO"]), 
	f.sum(dfData["SALARIO"]), 
	f.max(dfData["SALARIO"])
)

#Mostramos los datos
df5.show()

#Revisemos el esquema, notamos que las columnas reciben nombre "extraños"
df5.printSchema()

#Colocando alias
df6 = dfData.groupBy(dfData["EDAD"]).agg(
	f.count(dfData["EDAD"]).alias("CANTIDAD"),
	f.min(dfData["FECHA_INGRESO"]).alias("FECHA_CONTRATO_MAS_RECIENTE"),
	f.sum(dfData["SALARIO"]).alias("SUMA_SALARIOS"),
	f.max(dfData["SALARIO"]).alias("SALARIO_MAYOR")
)

#Mostramos los datos
df6.show()

#Revisamos el esquema
df6.printSchema()


#Ordenar ascendentemente por un campo
df7 = dfData.sort(dfData["EDAD"].asc())

#Mostramos los datos
df7.show()

#Ordenar ascendentemente y descendentemente por más de un campo
df8 = dfData.sort(dfData["EDAD"].asc(), dfData["SALARIO"].desc())

#Mostramos los datos
df8.show()



#Lectura de PERSONA
dfPersona = spark.sql("SELECT * FROM TEMPORAL.CLIENTE")

#Mostramos los datos
dfPersona.show()

#Lectura de TRANSACCIONES
dfTransaccion = spark.sql("SELECT * FROM TEMPORAL.TRANSACCION")

#Mostramos los datos
dfTransaccion.show()

#Ejecución del JOIN
dfJoin = dfTransaccion.alias("T").join(
	dfPersona.alias("P"), 
	f.col("T.ID_PERSONA") == f.col("P.ID")
).select(
	"P.NOMBRE", 
	"P.EDAD", 
	"P.SALARIO", 
	"T.MONTO", 
	"T.FECHA"
)

dfJoin.show()



#Agrupamos los datos
df1 = dfData.groupBy(dfData["EDAD"]).agg(
    f.count(dfData["EDAD"]).alias("CANTIDAD"),
    f.min(dfData["FECHA_INGRESO"]).alias("FECHA_CONTRATO_MAS_RECIENTE"),
    f.sum(dfData["SALARIO"]).alias("SUMA_SALARIOS"),
    f.max(dfData["SALARIO"]).alias("SALARIO_MAYOR")
)

#Mostramos los datos
df1.show()

#Filtramos por una EDAD
df2 = df1.filter(df1["EDAD"] > 35)

#Mostramos los datos
df2.show()

#Agregamos un filtro de SUMA_SALARIOS
df3 = df2.filter(df2["SUMA_SALARIOS"] > 50000)

#Mostramos los datos
df3.show()





#También podríamos hacerlo desde sólo un paso
dfResultado = dfData.groupBy(dfData["EDAD"]).agg(
    f.count(dfData["EDAD"]).alias("CANTIDAD"),
    f.min(dfData["FECHA_INGRESO"]).alias("FECHA_CONTRATO_MAS_RECIENTE"),
    f.sum(dfData["SALARIO"]).alias("SUMA_SALARIOS"),
    f.max(dfData["SALARIO"]).alias("SALARIO_MAYOR")
).alias("D").\
filter(f.col("D.EDAD") > 35).\
filter(f.col("D.SUMA_SALARIOS") > 5000).\
filter(f.col("D.SALARIO_MAYOR") > 1000)

dfResultado.show()





## ESCRITURA:
import pyspark.sql.functions as f
#Leemos los datos con SPARK SQL
dfData = spark.sql("SELECT * FROM TEMPORAL.CLIENTE")

#Agrupamos los datos
dfResultado = dfData.groupBy(dfData["EDAD"]).agg(
    f.count(dfData["EDAD"]).alias("CANTIDAD"),
    f.min(dfData["FECHA_INGRESO"]).alias("FECHA_CONTRATO_MAS_RECIENTE"),
    f.sum(dfData["SALARIO"]).alias("SUMA_SALARIOS"),
    f.max(dfData["SALARIO"]).alias("SALARIO_MAYOR")
)

#Mostramos los datos
dfResultado.show()

#Almacenamiento
dfResultado.write.mode("overwrite").format("parquet").option("compression", "snappy").save("/proyectos/temporal/dfResultado")

#[HADOOP] CONSULTEMOS EN HADOOP
#hdfs dfs -ls /proyectos/temporal/dfResultado


#Leemos la carpeta
dfResultadoRead = spark.read.format("parquet").load("/proyectos/temporal/dfResultado")

#Mostramos los datos
dfResultadoRead.show()



#################################
# INSERCION DE LA INFORMACION EN LA TABLA TABON_TRANSACCIONES

import pyspark.sql.functions as f

dfPersona = spark.sql('SELECT * FROM TEMPORAL.CLIENTE')
dfEmpresa = spark.sql('SELECT * FROM TEMPORAL.EMPRESA')
dfTransaccion = spark.sql('SELECT * FROM TEMPORAL.TRANSACCION')

# Procesamiento

#PASO 1: OBTENER LOS DATOS DE LA PERSONA QUE REALIZÓ LA TRANSACCIÓN
df1 = dfPersona.alias("P").join(
	dfEmpresa.alias("E"), 
	f.col("P.ID_EMPRESA") == f.col("E.ID")
).select(
	f.col("P.ID").alias("ID_PERSONA"),
	f.col("P.NOMBRE").alias("NOMBRE_PERSONA"),
	f.col("P.EDAD").alias("EDAD_PERSONA"),
	f.col("P.SALARIO").alias("SALARIO_PERSONA"),
	f.col("E.NOMBRE").alias("TRABAJO_PERSONA")
)
df1.show()


#PASO 2: OBTENER EL NOMBRE DE LA EMPRESA EN DONDE TRABAJA LA PERSONA
df2 = df1.alias("P").join(
	dfTransaccion.alias("T"), 
	f.col("P.ID_PERSONA") == f.col("T.ID_PERSONA")
).select(
	f.col("P.ID_PERSONA").alias("ID_PERSONA"),
	f.col("P.NOMBRE_PERSONA").alias("NOMBRE_PERSONA"),
	f.col("P.EDAD_PERSONA").alias("EDAD_PERSONA"),
	f.col("P.SALARIO_PERSONA").alias("SALARIO_PERSONA"),
	f.col("P.TRABAJO_PERSONA").alias("TRABAJO_PERSONA"),
	f.col("T.ID_EMPRESA").alias("ID_EMPRESA_TRANSACCION"),
	f.col("T.MONTO").alias("MONTO_TRANSACCION"),
	f.col("T.FECHA").alias("FECHA_TRANSACCION")
)

#Mostramos los datos
df2.show()

#PASO 3: OBTENER EL NOMBRE DE LA EMPRESA EN DONDE SE REALIZÓ LA TRANSACCIÓN
dfResultado = df2.alias("P").join(
	dfEmpresa.alias("E"), 
	f.col("P.ID_EMPRESA_TRANSACCION") == f.col("E.ID")
).select(
	f.col("P.ID_PERSONA").alias("ID_PERSONA"),
	f.col("P.NOMBRE_PERSONA").alias("NOMBRE_PERSONA"),
	f.col("P.EDAD_PERSONA").alias("EDAD_PERSONA"),
	f.col("P.SALARIO_PERSONA").alias("SALARIO_PERSONA"),
	f.col("P.TRABAJO_PERSONA").alias("TRABAJO_PERSONA"),
	f.col("P.MONTO_TRANSACCION").alias("MONTO_TRANSACCION"),
	f.col("P.FECHA_TRANSACCION").alias("FECHA_TRANSACCION"),
	f.col("E.NOMBRE").alias("EMPRESA_TRANSACCION")
)

dfResultado.show()
dfResultado.createOrReplaceTempView("dfResultado")

#Almacenamos el resultado en Hive
spark.sql('TRUNCATE TABLE TEMPORAL.TABLON_TRANSACCION SELECT * FROM dfResultado')
spark.sql('INSERT INTO TEMPORAL.TABLON_TRANSACCIONES SELECT * FROM dfResultado')


dfResultado.printSchema()

dfTransacciones = spark.sql('SELECT * FROM TEMPORAL.TABLON_TRANSACCIONES')
dfTransacciones.show()



--- matar sesiones kill desde consola HDFS
yarn application -list
yarn application -kill application_1629543009811_0005











