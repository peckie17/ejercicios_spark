---
title: Ejercicio Ecommerce 1
aliases: []
tags: [zophia/reto/ecommerce1,DE,spark]
date: 2022-02-03
---
# Ejercicio Ecommerce 1

## 0 Importar Archivos
- Se importaron los archivos, todas las tablas, desde el bucket de Cloud Storage que contienen los datasets.
- Al cargar los archivos se especificó que eran multilínea, dado que se podía interpretar como si los saltos de línea que contuvieran las columnas, fueran nuevos registros/filas. Además de especificar que el delimitador era `|`.
Por ejemplo:
 ```python
clientes = spark.read.options(header='True', inferSchema='True', delimiter='|',multiline='True').csv( main_ds_path + 'detalle_cliente.csv' )
 ```

## 1 Quitar acentos y saltos de línea
- Se usó la función [`translate`]([pyspark.sql.functions.translate — PySpark 3.2.1 documentation (apache.org)](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.translate.html)) para mapear las vocales con acento a la versión sin acento, así como el caracter `'\n'` a un espacio. Y se le dio el alias de la columna.
```python
translate( col(c),'áéíóúñ\n','aeioun ').alias(c)
```

### Aplicar transformación a todas las columnas
- Esta transformación se aplicó a todas las columna de las tablas usando un "list comprehension", ciclando por todas las columnas.

```python
[ translate( col(c),'áéíóúñ\n','aeioun ').alias(c) for c in clientes.columns]
```

- Es posible que usar un `for` para iterar sobre las columnas sea ineficiente, y sería mejor usar un `map`, pero eso implica la conversión del DF a un RDD, haciendo que no sea posible el acceso al optimizador.
	- Se encontraron otras opciones en: [Performing operations on multiple columns in a PySpark DataFrame | by Matthew Powers | Medium](https://mrpowers.medium.com/performing-operations-on-multiple-columns-in-a-pyspark-dataframe-36e97896c378), pero la conclusión es que las 3 opciones presentadas conllevan al mismo plan físico.
- Finalmente, para generar el nuevo DF resultante de la transformación se empleó `select` en vez de `withColumn`, ya que esta última no se recomienda para aplicar sobre varias transformaciones consecutivas, y conlleva un costo en el análisis. Además de que se genera un nuevo DF con la adición de una nueva columna.
	- [The hidden cost of Spark withColumn | by Manu Zhang | Medium](https://medium.com/@manuzhang/the-hidden-cost-of-spark-withcolumn-8ffea517c015)

```python
clientes_1 = clientes.select( [ translate( col(c),'áéíóúñ\n','aeioun ').alias(c) for c in clientes.columns] )
```

- Se hizo lo mismo para cada tabla, aunque esto es impráctico dado que se está repitiendo lo mismo, una mala práctica de software.

## 2 Separar dirección
- Para localizar la parte correspondiente al estado y código postal de la direeción, era necesario localizar la última coma de la cadena. Debido a diferentes razones:
	- El estado y CP no tenían una longitud regular, ni expresión regular.
	- El estado podía contener espacios, tal es el caso de "Q. Roo", por lo que no se podía identificar como una sola palabra.
	- El código postal podía contener guiones dentro del bloque de números.
	- Tampoco era viable hacer el *split* en cualquier coma, porque dentro de la dirección pricipal también podía haber comas.
		- Esto resultaría en un número incosistente de columnas para cada fila.
- Se optó por usar la función `split`, usando la expresión regular que coincidiera con la última coma de la línea (junto con el espacio) `',\s?(?=[^,]*$)'`.
	- Había más opciones, pero al final todas implicaban usar una regex para extraer y hacer otros reemplazos posteriores.

```python
col_split = split(clientes_1.direccion, ',\s?(?=[^,]*$)')
```

- Dado que split regresa una columna conteniendo una lista con los diferentes elementos que fueron separados, se obtuvo la primera parte con `getItem(i)`. Aunque también se pudo haber accedido con `[i]`.

```python
col_dir_calle = ( col_split.getItem(0) ).alias('direccion')
```

- Un procedimiento similar fue para separar la parte de estado y código postal, solo que esta vez se hizo el *split* en base al último espacio.

```python
col_dir_edo_cp = split( col_split.getItem(1), '\s(?=[^\s]*$)' )
```

- Finalmente, se creó el nuevo DF usando las columnas ya transformadas.
	- El `*` es para desempacar los elementos en la lista.
```python
clientes_2 = clientes_1.select(*cols_clientes[:2], col_dir_calle, col_dir_edo, col_dir_cp, *cols_clientes[3:])
```

## 3 Separar el estado de la sucursal
- En este caso, solo se requería extraer el estado (y crear una nueva columna) de la cadena de la columna sucursal, sin afectar el contenido de esta columna original.
- Por esto, se usó la función `substring_index`, para extraer estado, es decir, todo lo que se hallaba a la izquierda del `-`, y por eso se usó `1` como último argumento.
```python
col_estado = substring_index(col('sucursal'), '-', 1).alias('estado')
```

- Finalmente, usamos `withColumn` para agregar la nueva columna.
```python
sucursal_2 = sucursal_1.withColumn('estado', col_estado)
```

## 4 Guardar resultados
- Los resultados fueron guardados como *csv* en un bucket, usando el modo *overwrite* y sin compresión.

```python
clientes_2.write.format("csv") \
.mode("overwrite") \
.option("header", "true") \
.save(main_out_path + "ecommerce_1_clientes")
```

## Algunos problemas
- Usando Spark de forma local, en general no hubo problemas con las transformaciones, pero al intentar exportar los resultados a un archivo surgían errores por una mala configuración de Hadoop en Windows.
- Se ejecutó el script en un clúster de Dataproc, y si bien no hubo problemas con guardar los resultados de las transformaciones, sí los hubo en algunas transformaciones.
	- La función `split` marcaba error por un tercer argumento posicional, diciendo que solo debería haber 2, aunque la documentación indica 3 (el último como opcional).
		- Este error se atribuye a una diferencia de versiones de PySpark configurada en el clúster y en local.
	- Al querer seleccionar la última columna de la tabla sucursal, `'telefono'`, se obtenía un error de que no podía resolver la columna `telefono\r`. Este `'\r'` es un caracter especial de Windows para saltos de línea.
		- Se atribuye a la diferencia de codificaciones de Window y el SO con el que trabaja el clúster.
		- Podría ser reparado haciendo la conversión del script con `dos2unix`
