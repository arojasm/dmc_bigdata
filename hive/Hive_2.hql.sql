-- clase de Big Data - 08/2021
--   
-- Consola Hive: Ingresar a consola Hive con el cliente BEELINE
beeline -u jdbc:hive2://

-- Consola Hive : En HIVE, listamos las bases de datos existentes
SHOW DATABASES;


--Consola Hive : Creamos database en un ruta pre definida e incluimos propiedades
CREATE DATABASE IF NOT EXISTS TEMPORAL 
COMMENT 'Base de datos temporal'
LOCATION '/proyectos/temporal'
WITH DBPROPERTIES ('creator' = 'Arturo Rojas', 'date' = '2021-08-14');

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



--Consola Hive : 
-- Verificamos
SHOW TABLES IN TEMPORAL;

--Consola LINUX - HDFS : 
-- En HDFS, listar el contenido de la carpetra 
hdfs dfs -ls /proyectos/temporal

--Consola LINUX - HDFS : 
-- En HDFS, subir el archivo LINUX 
hdfs dfs -put dataset/cliente.data /proyectos/temporal/cliente

--Consola HIVE : 
-- En HIVE, mostramos algunos registros de la tabla
SELECT * FROM TEMPORAL.CLIENTE LIMIT 10;
DESC TEMPORAL.CLIENTE;
DESC FORMATTED TEMPORAL.CLIENTE;



---------------------------------------------------
---------------------------------------------------
--- FORMATOS DE ARCHIVOS:
-- ORC , PARQUET, AVRO

--- FORMATO ORC:
-- FORMATO ORC

-- Creación de una tabla en ORC
-- CONSOLA HIVE:
CREATE EXTERNAL TABLE TEMPORAL.CLIENTE_ORC(
ID_CLIENTE STRING,
NOMBRE STRING,
TELEFONO STRING,
CORREO STRING,
FECHA_INGRESO DATE,
EDAD INT,
SALARIO DOUBLE,
ID_EMPRESA STRING
)
STORED AS ORC
TBLPROPERTIES ('creator'='Arturo Rojas', 'created_at'='2021-08-14');

-- CONSOLA HIVE:
INSERT OVERWRITE TABLE TEMPORAL.CLIENTE_ORC
SELECT * FROM  TEMPORAL.CLIENTE;

-- CONSOLA HIVE:
SELECT * FROM TEMPORAL.CLIENTE_ORC LIMIT 10;




----------------------------------------
-------------------------------------------
-- FORMATO PARQUET

-- CONSOLA HIVE:
CREATE EXTERNAL TABLE IF NOT EXISTS TEMPORAL.CLIENTE_PARQUET(
ID STRING,
NOMBRE STRING COMMENT "Nombre de clientes", 
TELEFONO STRING COMMENT "Telefono de clientes",
CORREO STRING,
FECHA_INGRESO STRING,
EDAD INT,
SALARIO DOUBLE,
ID_EMPRESA STRING
)
COMMENT 'Tabla de clientes external parquet'
STORED AS PARQUET
TBLPROPERTIES ('creator'='Arturo Rojas', 'created_at'='2021-08-14');

-- CONSOLA HIVE:
INSERT OVERWRITE TABLE TEMPORAL.CLIENTE_PARQUET
SELECT * FROM TEMPORAL.CLIENTE;



----------------------------------------
-------------------------------------------
-- FORMATO AVRO

-- consola LINUX:
-- creart carpeta en linux:
mkdir esquema_avro
-- crear carpeta esquema_avro en HDFS
hdfs dfs -mkdir /proyectos/esquema_avro
-- copiar esquemas avro al HDFS
hdfs dfs -put esquema_avro/cliente_avro.avsc /proyectos/esquema_avro



--CONSOLA HIVE:
-- CREAR LA TABLA CLIENTES FORMATO AVRO
DROP TABLE TEMPORAL.CLIENTE_AVRO;
CREATE EXTERNAL TABLE IF NOT EXISTS TEMPORAL.CLIENTE_AVRO
STORED AS AVRO
TBLPROPERTIES ('avro.schema.url'='hdfs:///proyectos/esquema_avro/cliente_avro.avsc', 'created_at'='2021-08-14');


--- insertar data de la tabla textfile cliente_ hacia la nueva tabla cliente _avro
-- CONSOLA HIVE
INSERT OVERWRITE TABLE TEMPORAL.CLIENTE_AVRO
SELECT * FROM TEMPORAL.CLIENTE;

-- CONSOLA HIVE:
select * from TEMPORAL.CLIENTE LIMIT 5;





-----------------------------------------------------
------------------------------------------------------
--- COMPRESION DE DATOS - SNAPY

-- CONSOLA HIVE
CREATE TABLE TEMPORAL.CLIENTE_ORC_SNAPPY(
ID_CLIENTE STRING,
NOMBRE STRING,
TELEFONO STRING,
CORREO STRING,
FECHA_INGRESO DATE,
EDAD INT,
SALARIO DOUBLE,
ID_EMPRESA STRING
)
STORED AS ORC
TBLPROPERTIES ("orc.compression"="SNAPPY");

-- CONSOLA HIVE
SET hive.exec.compress.output=true;
SET orc.compression=SNAPPY;

-- CONSOLA HIVE
INSERT OVERWRITE TABLE TEMPORAL.CLIENTE_ORC_SNAPPY
SELECT * FROM  TEMPORAL.CLIENTE;

-- CONSOLA HIVE
-- Verificamos
SELECT * FROM TEMPORAL.CLIENTE_ORC_SNAPPY LIMIT 10;

