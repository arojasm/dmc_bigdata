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

