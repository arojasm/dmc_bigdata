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
NOMBRE_EMPRESA STRING,
MONTO_TRANSACCION DOUBLE,
FECHA_TRANSACCION STRING
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
