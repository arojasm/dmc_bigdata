/*****************************************/
---- SENTENCIAS HQL

CREATE DATABASE IF NOT EXISTS OPERACIONAL
COMMENT 'Base de datos OPERACIONAL'
LOCATION '/proyectos/operacional'
WITH DBPROPERTIES ('creator' = 'Arturo Rojas', 'date' = '2021-08-14');


DROP TABLE OPERACIONAL.FACT_TRANSACCIONES;

CREATE EXTERNAL TABLE OPERACIONAL.FACT_TRANSACCIONES(
ID_CLIENTE STRING,
NOMBRE_CLIENTE STRING,
EDAD_CLIENTE INT,
SALARIO_CLIENTE DOUBLE,
ID_EMPRESA STRING,
NOMBRE_EMPRESA STRING,
MONTO_TRANSACCION DOUBLE
)
PARTITIONED BY (FECHA_TRANSACCION STRING)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compression"="SNAPPY");



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
TBLPROPERTIES ('creator'='Arturo Rojas', 'created_at'='2021-08-14', 'skip.header.line.count'='1');

-- CONSOLODA LINUX
-- cargamos la data
hdfs dfs -put dataset/empresa.data /proyectos/temporal/empresa

--Consola HIVE : 
select * from TEMPORAL.EMPRESA;

----------------------------------------
INSERT OVERWRITE TABLE OPERACIONAL.FACT_TRANSACCIONES
PARTITION (FECHA_TRANSACCION) 
SELECT
T.ID_CLIENTE ID_CLIENTE ,
UPPER(CL.NOMBRE)  NOMBRE_CLIENTE ,
CAST (CL.EDAD AS INT) EDAD_CLIENTE ,
CAST (CL.SALARIO AS DOUBLE)  SALARIO_CLIENTE,
T.ID_EMPRESA ID_EMPRESA ,
UPPER(E.NOMBRE) NOMBRE_EMPRESA ,
CAST (T.MONTO AS DOUBLE) MONTO_TRANSACCION,
T.FECHA
FROM
TEMPORAL.transaccion_particion_dinamica T
JOIN TEMPORAL.CLIENTE_PARQUET_SNAPPY CL ON T.ID_CLIENTE= CL.ID_CLIENTE
JOIN TEMPORAL.EMPRESA E ON T.ID_EMPRESA = E.ID;

--LIMIT 5;


SELECT
T.ID_CLIENTE ID_CLIENTE ,
UPPER(CL.NOMBRE)  NOMBRE_CLIENTE ,
CAST (CL.EDAD AS INT) EDAD_CLIENTE ,
CONCAT(CL.NOMBRE, ' TIENE ', CL.EDAD , ' AÑOS DE EDAD' )  NOMBRE_CLIENTE ,
CAST (CL.SALARIO AS DOUBLE)  SALARIO_CLIENTE,
T.ID_EMPRESA ID_EMPRESA ,
UPPER(E.NOMBRE) NOMBRE_EMPRESA ,
CAST (T.MONTO AS DOUBLE) MONTO_TRANSACCION,
ROUND (T.MONTO * 1.18) MONTO_IGV,
T.FECHA
FROM
TEMPORAL.transaccion_particion_dinamica T
JOIN TEMPORAL.CLIENTE_PARQUET_SNAPPY CL ON T.ID_CLIENTE= CL.ID_CLIENTE
JOIN TEMPORAL.EMPRESA E ON T.ID_EMPRESA = E.ID
LIMIT 5;

+-------------+-----------------+---------------+---------------------------------+------------------+-------------+-----------------+--------------------+------------+-------------+
| id_cliente  | nombre_cliente  | edad_cliente  |         nombre_cliente          | salario_cliente  | id_empresa  | nombre_empresa  | monto_transaccion  | monto_igv  |   t.fecha   |
+-------------+-----------------+---------------+---------------------------------+------------------+-------------+-----------------+--------------------+------------+-------------+
| 87          | KARLY           | 25            | Karly TIENE 25 AÑOS DE EDAD     | 3715.0           | 8           | HP              | 2409.0             | 2843.0     | 2018-01-23  |
| 87          | KARLY           | 25            | Karly TIENE 25 AÑOS DE EDAD     | 3715.0           | 8           | HP              | 2409.0             | 2843.0     | 2018-01-23  |
| 72          | TALLULAH        | 46            | Tallulah TIENE 46 AÑOS DE EDAD  | 9867.0           | 8           | HP              | 2154.0             | 2542.0     | 2018-01-23  |
| 72          | TALLULAH        | 46            | Tallulah TIENE 46 AÑOS DE EDAD  | 9867.0           | 8           | HP              | 2154.0             | 2542.0     | 2018-01-23  |
| 51          | DAMON           | 49            | Damon TIENE 49 AÑOS DE EDAD     | 2669.0           | 8           | HP              | 1169.0             | 1379.0     | 2018-01-23  |
+-------------+-----------------+---------------+---------------------------------+------------------+-------------+-----------------+--------------------+------------+-------------+


--- funciones caracter:
--OPERADORES DE PREDICADO LIKE 
SELECT 
C.NOMBRE
FROM 
TEMPORAL.CLIENTE_PARQUET_SNAPPY C
WHERE 
NOMBRE LIKE 'J%' limit 10;
+-----------+
| c.nombre  |
+-----------+
| Jocelyn   |
| Jonah     |
| Jana      |
| Jin       |
| Jennifer  |
| Jillian   |
| Joy       |
| Jack      |
| Jayme     |
| Jocelyn   |
+-----------+



SELECT 
C.NOMBRE
FROM 
TEMPORAL.CLIENTE_PARQUET_SNAPPY C
WHERE 
NOMBRE LIKE '%n' limit 10;
+-----------+
| c.nombre  |
+-----------+
| Jocelyn   |
| Aidan     |
| Cadman    |
| Allen     |
| Alden     |
| Owen      |
| Samson    |
| Brenden   |
| Stephen   |
| Clayton   |
+-----------+



SELECT 
C.NOMBRE
FROM 
TEMPORAL.CLIENTE_PARQUET_SNAPPY C
WHERE 
NOMBRE LIKE '%w%' limit 10;

+-----------+
| c.nombre  |
+-----------+
| Owen      |
| Owen      |
+-----------+


SELECT 
C.NOMBRE,
CASE 
WHEN C.NOMBRE LIKE 'J%' THEN ' Empieza con J' 
WHEN C.NOMBRE LIKE '%n' THEN ' Termina con n'
WHEN C.NOMBRE LIKE '%n' THEN ' Termina con n'
WHEN C.NOMBRE LIKE '%w%' THEN ' Contiene w'
ELSE ' Otras conbinaciones'
END
FROM 
TEMPORAL.CLIENTE_PARQUET_SNAPPY C 
 limit 20;


+------------+-----------------------+
|  c.nombre  |          _c1          |
+------------+-----------------------+
| Priscilla  |  Otras conbinaciones  |
| Jocelyn    |  Empieza con J        |
| Aidan      |  Termina con n        |
| Leandra    |  Otras conbinaciones  |
| Bert       |  Otras conbinaciones  |
| Mark       |  Otras conbinaciones  |
| Jonah      |  Empieza con J        |
| Hanae      |  Otras conbinaciones  |
| Cadman     |  Termina con n        |
| Melyssa    |  Otras conbinaciones  |
| Tanner     |  Otras conbinaciones  |
| Trevor     |  Otras conbinaciones  |
| Allen      |  Termina con n        |
| Wanda      |  Otras conbinaciones  |
| Alden      |  Termina con n        |
| Omar       |  Otras conbinaciones  |
| Owen       |  Termina con n        |
| Laura      |  Otras conbinaciones  |
| Emery      |  Otras conbinaciones  |
| Carissa    |  Otras conbinaciones  |
+------------+-----------------------+




-- funciones de agregación

select sum(salario), MIN(salario), MAX(salario) from TEMPORAL.CLIENTE_PARQUET_SNAPPY limit 10;

select sum(CAST(salario AS DOUBLE)) AS SUMA_, MIN(CAST(salario AS DOUBLE)) AS MIN_, MAX(CAST(salario AS DOUBLE)) AS MAX_  from temporal.CLIENTE_PARQUET_SNAPPY;


--
-- funciones de agregación;
SELECT 
EDAD, 
AVG (SALARIO) AS SALARIO_PROMEDIO, 
COUNT(*) conteo_personas
FROM TEMPORAL.CLIENTE_PARQUET_SNAPPY 
GROUP BY EDAD 
HAVING EDAD > 30
ORDER BY EDAD DESC
LIMIT 20;

+-------+---------------------+------+------+
| edad  |  salario_promedio   | _c2  | _c3  |
+-------+---------------------+------+------+
| 70    | 12105.0             | 6    | 6    |
| 69    | 6834.0              | 2    | 2    |
| 67    | 15119.0             | 6    | 6    |
| 64    | 20925.5             | 4    | 4    |
| 63    | 4963.0              | 2    | 2    |
| 61    | 21452.0             | 2    | 2    |
| 60    | 6851.0              | 2    | 2    |
| 59    | 9944.5              | 4    | 4    |
| 58    | 15065.0             | 6    | 6    |
| 57    | 3833.6666666666665  | 6    | 6    |
| 56    | 6515.0              | 2    | 2    |
| 55    | 11961.5             | 4    | 4    |
| 54    | 4588.0              | 2    | 2    |
| 53    | 5876.5              | 4    | 4    |
| 52    | 10791.0             | 6    | 6    |
| 51    | 6009.0              | 4    | 4    |
| 49    | 2669.0              | 2    | 2    |
| 48    | 14609.0             | 4    | 4    |
| 47    | 17518.0             | 4    | 4    |
| 46    | 16410.0             | 4    | 4    |
+-------+---------------------+------+------+



-- creacion de vistas en HIVE

CREATE VIEW V_CLIENTE_SALARIO 
AS
SELECT 
EDAD, 
AVG (SALARIO) AS SALARIO_PROMEDIO, 
COUNT(*) conteo_personas
FROM TEMPORAL.CLIENTE_PARQUET_SNAPPY 
GROUP BY EDAD 
HAVING EDAD > 30
ORDER BY EDAD DESC
LIMIT 20;



SELECT 
NOMBRE, 
SALARIO, 
CASE WHEN CAST(SALARIO AS DOUBLE) < 8000.0 THEN "BAJO" 
WHEN CAST(P.SALARIO AS DOUBLE) >= 8000.0 AND CAST(P.SALARIO AS DOUBLE) < 12000 THEN "MEDIO" 
WHEN CAST(P.SALARIO AS DOUBLE) >= 12000.0 AND CAST(P.SALARIO AS DOUBLE) < 25000 THEN "ALTO" 
ELSE "MUY ALTO" END AS TIPO_SALARIO 
FROM TEMPORAL.CLIENTE_PARQUET_SNAPPY P LIMIT 10;

+------------+----------+---------------+
|   nombre   | salario  | tipo_salario  |
+------------+----------+---------------+
| Priscilla  | 9298.0   | MEDIO         |
| Jocelyn    | 10853.0  | MEDIO         |
| Aidan      | 3387.0   | BAJO          |
| Leandra    | 22102.0  | ALTO          |
| Bert       | 7800.0   | BAJO          |
| Mark       | 8112.0   | MEDIO         |
| Jonah      | 17040.0  | ALTO          |
| Hanae      | 6834.0   | BAJO          |
| Cadman     | 7996.0   | BAJO          |
| Melyssa    | 4913.0   | BAJO          |
+------------+----------+---------------+


CREATE VIEW V_CLIENTE_TIPO_SALARIO 
AS
SELECT 
NOMBRE, 
SALARIO, 
CASE WHEN CAST(SALARIO AS DOUBLE) < 8000.0 THEN "BAJO" 
WHEN CAST(P.SALARIO AS DOUBLE) >= 8000.0 AND CAST(P.SALARIO AS DOUBLE) < 12000 THEN "MEDIO" 
WHEN CAST(P.SALARIO AS DOUBLE) >= 12000.0 AND CAST(P.SALARIO AS DOUBLE) < 25000 THEN "ALTO" 
ELSE "MUY ALTO" END AS TIPO_SALARIO 
FROM TEMPORAL.CLIENTE_PARQUET_SNAPPY P;

