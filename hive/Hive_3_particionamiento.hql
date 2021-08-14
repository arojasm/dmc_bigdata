--
-- Tablas particionadas HIVE
--


-- CONSOLA DE HIVE:
--CREACION TABLA PARTICIONDA POR CAMPO FECHA.
CREATE EXTERNAL TABLE TEMPORAL.TRANSACCION(
ID_CLIENTE STRING,
ID_EMPRESA STRING,
MONTO DOUBLE
)
PARTITIONED BY (FECHA STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
TBLPROPERTIES ('creator'='Arturo Rojas', 'created_at'='2021-08-14 09:00:00', 'skip.header.line.count'='1');

-----------------------------------
-- CONSOLA DE HIVE:
--CARGAR LA DATA DEL DIA 21.
LOAD DATA LOCAL INPATH 'dataset/transacciones-2018-01-21.data' OVERWRITE INTO TABLE TEMPORAL.TRANSACCION PARTITION (FECHA='2018-01-21');

-- CONSOLA DE HIVE:
-- Verificamos que haya datos
SELECT * FROM TEMPORAL.TRANSACCION LIMIT 10;

-- CONSOLA DE HIVE:
-- Mostramos la particiones existentes
SHOW PARTITIONS TEMPORAL.TRANSACCION;

-------------------------------
-- CONSOLA DE HIVE:
--CARGAR LA DATA DEL DIA 22.
LOAD DATA LOCAL INPATH 'dataset/transacciones-2018-01-22.data' OVERWRITE INTO TABLE TEMPORAL.TRANSACCION PARTITION (FECHA='2018-01-22');

-- CONSOLA DE HIVE:
-- Verificamos que haya datos
SELECT * FROM TEMPORAL.TRANSACCION LIMIT 10;

-- CONSOLA DE HIVE:
-- Mostramos la particiones existentes
SHOW PARTITIONS TEMPORAL.TRANSACCION;

----------------------------------
-- CONSOLA DE LINUX - HDFS 
--CARGAR LA DATA DEL DIA 23.

-- Listemos el contenido de la carpeta HDFS de nuestra tabla
hdfs dfs -ls /proyectos/temporal/transaccion

hdfs dfs -ls /proyectos/temporal/transaccion/fecha=2018-01-21

-- En la ruta HDFS de nuestra tabla, creamos la siguiente carpeta:
hdfs dfs -mkdir -p /proyectos/temporal/transaccion/fecha=2018-01-23

-- Subimos el archivo de transacciones de ese día
hdfs dfs -put dataset/transacciones-2018-01-23.data /proyectos/temporal/transaccion/fecha=2018-01-23

-- CONSOLA HIVE:
-- Verificamos si existe la partición
SHOW PARTITIONS TEMPORAL.TRANSACCION;

-- CONSOLA HIVE:
-- REPARAMOS LA TABLA HIVE PARA QUE AGREGE UNA PARTICION MAS
MSCK REPAIR TABLE TEMPORAL.TRANSACCION;



