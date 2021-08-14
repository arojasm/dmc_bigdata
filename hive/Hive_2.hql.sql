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
WITH DBPROPERTIES ('creator' = 'Arturo Rojas', 'date' = '2021-08-07');

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

--Consola HIVE : 
-- crear una tabla con la propiedad de saltar la primera linea
CREATE TABLE IF NOT EXISTS TEMPORAL.CLIENTE_skip(
ID STRING,
NOMBRE STRING COMMENT "Nombre de clientes", 
TELEFONO STRING COMMENT "Telefono de clientes",
CORREO STRING,
FECHA_INGRESO STRING,
EDAD INT,
SALARIO DOUBLE,
ID_EMPRESA STRING
)
COMMENT 'Tabla de clientes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ('creator'='Arturo Rojas', 'created_at'='2020-04-18', 'skip.header.line.count'='1');

--Consola LINUX - HDFS : 
-- subir la data desde linux ( dataset/cliente.data)  hacia HDFS (/proyectos/temporal/cliente_skip)
hdfs dfs -put dataset/cliente.data /proyectos/temporal/cliente_skip

--Consola HIVE : 
-- En HIVE, mostramos algunos registros de la tabla
SELECT * FROM TEMPORAL.CLIENTE_skip LIMIT 10;

--Consola HIVE : 
DROP TABLE TEMPORAL.CLIENTE_skip;

--Consola HIVE : 
CREATE TABLE IF NOT EXISTS TEMPORAL.CLIENTE_skip(
ID STRING,
NOMBRE STRING COMMENT "Nombre de clientes", 
TELEFONO STRING COMMENT "Telefono de clientes",
CORREO STRING,
FECHA_INGRESO STRING,
EDAD INT,
SALARIO DOUBLE,
ID_EMPRESA STRING
)
COMMENT 'Tabla de clientes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ('creator'='Arturo Rojas', 'created_at'='2020-04-18', 'skip.header.line.count'='1');


--Consola HIVE : 
-- CARGAR DATA DIRECTO A UNA TABLA:
LOAD DATA LOCAL INPATH 'dataset/cliente.data' INTO TABLE TEMPORAL.CLIENTE_skip;

--Consola HIVE : 
-- Consultar la tabla
SELECT * FROM TEMPORAL.CLIENTE_skip LIMIT 10;


--Consola LINUX - HDFS : 
-- En HDFS, listar el contenido de la carpetra 
hdfs dfs -ls /proyectos/temporal/cliente_skip


----
---- CREAREMOS TABLA EXTERNAL HIVE
--Consola HIVE : 
DROP TABLE TEMPORAL.CLIENTE_skip_external;

--Consola HIVE : 
CREATE EXTERNAL TABLE IF NOT EXISTS TEMPORAL.CLIENTE_skip_external(
ID STRING,
NOMBRE STRING COMMENT "Nombre de clientes", 
TELEFONO STRING COMMENT "Telefono de clientes",
CORREO STRING,
FECHA_INGRESO STRING,
EDAD INT,
SALARIO DOUBLE,
ID_EMPRESA STRING
)
COMMENT 'Tabla de clientes skip external'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ('creator'='Arturo Rojas', 'created_at'='2020-04-18', 'skip.header.line.count'='1');


--Consola HIVE : 
-- CARGAR DATA DIRECTO A UNA TABLA:
LOAD DATA LOCAL INPATH 'dataset/cliente.data' INTO TABLE TEMPORAL.CLIENTE_skip_external;

--Consola HIVE : 
-- Consultar la tabla
SELECT * FROM TEMPORAL.CLIENTE_skip_external LIMIT 5;


--Consola LINUX - HDFS : 
-- En HDFS, listar el contenido de la carpetra 
hdfs dfs -ls /proyectos/temporal/cliente_skip

--Consola HIVE : 
DROP TABLE TEMPORAL.CLIENTE_skip_external;




-----------------------------------------------
------------------------------------------------

--Consola HIVE : 
-- creamos tabla empresa
CREATE EXTERNAL TABLE TEMPORAL.EMPRESA(
ID STRING,
NOMBRE STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ('creator'='Arturo Rojas', 'created_at'='2021-04-24', 'skip.header.line.count'='1');

-- cargamos la data
hdfs dfs -put dataset/empresa.data /proyectos/temporal/empresa

select * from TEMPORAL.EMPRESA;


---------------------------------------------
---------------------------------------------
-- formatos de archivos

-- formatos AVRO:

-- consola LINUX:
-- creart carpeta en linux:
mkdir esquema_avro

-- crear carpeta esquema_avro en HDFS
hdfs dfs -mkdir /proyectos/esquema_avro

-- copiar esquema avro al HDFS
hdfs dfs -put esquema_avro/cliente_avro.avsc /proyectos/esquema_avro


--CONSOLA HIVE:
-- CREAR LA TABLA CLIENTES FORMATO AVRO

CREATE EXTERNAL TABLE IF NOT EXISTS TEMPORAL.CLIENTE_AVRO
STORED AS AVRO
TBLPROPERTIES ('avro.schema.url'='hdfs:///proyectos/esquema_avro/cliente_avro.avsc', 'created_at'='2020-04-18', 'skip.header.line.count'='1');


--- insertar data de la tabla textfile cliente_skip_external hacia la nueva tabla clinte _avro
-- CONSOLA HIVE
INSERT OVERWRITE TABLE TEMPORAL.CLIENTE_AVRO
SELECT * FROM TEMPORAL.CLIENTE_skip_external;

-- CONSOLA HIVE:
select * from TEMPORAL.CLIENTE_AVRO LIMIT 5;

-- CONSOLA LINUX
hdfs dfs -ls /proyectos/temporal/cliente_avro


hdfs dfs -tail /proyectos/temporal/cliente_avro/000000_0
hdfs dfs -cat /proyectos/temporal/cliente_avro/000000_0