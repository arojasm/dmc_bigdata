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


