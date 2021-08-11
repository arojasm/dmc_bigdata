


-- comandos LINUX
-- listar 
ls
ls -lh

-- mostrar carpetas oculpas
ls -la  

-- crear archivos en linux
touch test.txt 

-- crear carpetas en linux
mkdir prueba

drwxr-xr-x 2 arturo arturo 4.0K Jul 24 17:45 prueba
-rw-r--r-- 1 arturo arturo    0 Jul 24 17:44 test.txt

la primera letra indica si es directorio ( d ) o archivo ( -) 
las primeras tres letras -> permisos del usuario dueño
las siguientes tres primeras  -> permisos para el grupo del dueño
las siguientes tres primeras  -> permisos para el resto de usuarios

el numero indica el # de copias
el usuario dueño
grupo del dueño
peso del recurso 
fechas modificacion
nombre del recurso

CARPETA RAIZ DE HDFS
-----------------------
hdfs dfs -ls /

--listar carpetas de user ( compn hadoop)
hdfs dfs -ls /user


-- para consultar los comandos realizados
history

-- para exportar los comandos en un archivo
history > comandos.txt

