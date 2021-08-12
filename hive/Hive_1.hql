-- clase de Big Data - 08/2021
--   
-- Ingresar a consola Hive con el cliente BEELINE

beeline -u jdbc:hive2://

-- En HIVE, listamos las bases de datos existentes
SHOW DATABASES;

-- Crear base de datos 
CREATE DATABASE IF NOT EXISTS TEMP;

-- verificamos
SHOW DATABASES;

--Creamos database en un ruta pre definida e incluimos propiedades
CREATE DATABASE IF NOT EXISTS TEMPORAL 
COMMENT 'Base de datos temporal'
LOCATION '/proyectos/temporal'
WITH DBPROPERTIES ('creator' = 'Arturo Rojas', 'date' = '2021-08-07');

CREATE DATABASE IF NOT EXISTS TEMPORAL2 
COMMENT 'Base de datos temporal'
WITH DBPROPERTIES ('creator' = 'Arturo Rojas', 'date' = '2021-08-07');

-- LISTAR VARIAS BASES DE DATOS
SHOW DATABASES LIKE 'TEM.*';

DESCRIBE DATABASE EXTENDED TEMPORAL;

--ELIMINAR BASE DE DATOS VACIAS:
DROP DATABASE IF EXISTS TEMPORAL;

DROP DATABASE IF EXISTS TEMPORAL2 CASCADE;

-- muestra los mensajes de error, comportamiento predetermindo, se debe eliminar primero las tablas:
DROP DATABASE IF EXISTS database_name RESTRICT;


-- ALTER DATABASE:
DESCRIBE DATABASE EXTENDED TEMPORAL;

-- añadiendo un propiedad clave - valor
ALTER DATABASE TEMPORAL SET 
DBPROPERTIES ( 'edited-by' = 'Arturo DBA');

-- En HIVE, creamos la tabla

CREATE TABLE IF NOT EXISTS TEMPORAL.CLIENTE(
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
STORED AS TEXTFILE;



-- Verificamos
SHOW TABLES IN TEMPORAL;


-- En HDFS, listar el contenido de la carpetra 

hdfs dfs -ls /proyectos/temporal


-- En HDFS, subir el archivo LINUX 

hdfs dfs -put dataset/cliente.data /proyectos/temporal/cliente
