from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master('yarn') \
    .appName('ecommerce ex1.2') \
    .getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

#%% LEER DATOS
main_ds_path = 'gs://zophia-datasets/ecommerce/'

clientes = spark.read.options(header='True', inferSchema='True', delimiter='|',multiline='True') \
  .csv( main_ds_path + 'detalle_cliente.csv' )

categoria = spark.read.options(header='True', inferSchema='True', delimiter='|',multiline='True') \
  .csv( main_ds_path + 'detalle_categoria.csv' )

detalle_productos = spark.read.options(header='True', inferSchema='True', delimiter='|',multiline='True') \
  .csv( main_ds_path + 'detalle_productos.csv' )

sucursal = spark.read.options(header='True', inferSchema='True', delimiter='|',multiline='True') \
  .csv( main_ds_path + 'detalle_sucursal.csv' )

ordenes = spark.read.options(header='True', inferSchema='True', delimiter='|',multiline='True') \
  .csv( main_ds_path + 'ordenes.csv' )

productos = spark.read.options(header='True', inferSchema='True', delimiter='|',multiline='True') \
  .csv( main_ds_path + 'productos.csv' )


#%% QUITAR ACENTOS
clientes_1 = clientes.select( [ translate( col(c),'áéíóúñ\n','aeioun ').alias(c) for c in clientes.columns] )
categoria_1 = categoria.select( [ translate( col(c),'áéíóúñ\n','aeioun ').alias(c) for c in categoria.columns] )
detalle_productos_1 = detalle_productos.select( [ translate( col(c),'áéíóú\n','aeiou ').alias(c) for c in detalle_productos.columns] )
sucursal_1 = sucursal.select( [ translate( col(c),'áéíóú\n','aeiou ').alias(c) for c in sucursal.columns] )
ordenes_1 = ordenes.select( [ translate( col(c),'áéíóú\n','aeiou ').alias(c) for c in ordenes.columns] )
productos_1 = productos.select( [ translate( col(c),'áéíóú\n','aeiou ').alias(c) for c in productos.columns] )

#%% CLIENTES - SEPARAR DIRECCION
col_split = split(clientes_1.direccion, ',\s?(?=[^,]*$)')
col_dir_calle = ( col_split.getItem(0) ).alias('direccion')

#separar edo y cp
col_dir_edo_cp = split( col_split.getItem(1), '\s(?=[^\s]*$)' ) 
col_dir_edo = col_dir_edo_cp.getItem(0).alias('estado')  #ahora hay que separar esto tmb en EDO y CP
col_dir_cp = col_dir_edo_cp.getItem(1).alias('codigo_postal')

cols_clientes = clientes_1.columns #columnas del original, para que no appendee más si lo vuelvo a correr
clientes_2 = clientes_1.select(*cols_clientes[:2], col_dir_calle, col_dir_edo, col_dir_cp, *cols_clientes[3:])

#clientes_2.select('*').show(truncate=True)
#%% DETALLE_SUCURSAL - OBTENER ESTADO
col_estado = substring_index(col('sucursal'), '-', 1).alias('estado')
#sucursal_2 = sucursal_1.select('sucursal_id','sucursal', col_estado, 'dirección', 'telefono')
sucursal_2 = sucursal_1.withColumn('estado', col_estado)

#%% ESCRIBIENDO A CSV
main_out_path = 'gs://bucket-naranja-2021/rebeca/ecommerce/output/'

clientes_2.write.format("csv") \
.mode("overwrite") \
.option("header", "true") \
.save(main_out_path + "ecommerce_1_clientes")

categoria_1.write.format("csv") \
.mode("overwrite") \
.option("header", "true") \
.save(main_out_path + "ecommerce_1_categoria")

detalle_productos_1.write.format("csv") \
.mode("overwrite") \
.option("header", "true") \
.save(main_out_path + "ecommerce_1_detalle_productos")

sucursal_2.write.format("csv") \
.mode("overwrite") \
.option("header", "true") \
.save(main_out_path + "ecommerce_1_sucursal")

ordenes_1.write.format("csv") \
.mode("overwrite") \
.option("header", "true") \
.save(main_out_path + "ecommerce_1_ordenes")

productos_1.write.format("csv") \
.mode("overwrite") \
.option("header", "true") \
.save(main_out_path + "ecommerce_1_productos")

