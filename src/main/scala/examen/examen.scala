package examen

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

object examen {

  /** Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   * Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
   * estudiantes (nombre, edad, calificación).
   * Realiza las siguientes operaciones:
   *
   * Muestra el esquema del DataFrame.
   * Filtra los estudiantes con una calificación mayor a 8.
   * Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */


  def ejercicio1(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    println("Esquema")
    df.printSchema()
    println("Calificaciones superiores a 8")
    df.filter("Calificacion > 8").show()
    println("Los nombres ordenados por calificacion")
    df.select("Nombre", "Calificacion").orderBy(col("Calificacion").desc).show()
    println("Dataframe")
    df.show()
    df
  }



  /**Ejercicio 2: UDF (User Defined Function)
   Pregunta: Define una función que determine si un número es par o impar.
   Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */

  def ejercicio2(numeros: DataFrame): DataFrame = {

    val ParOImpar: Int => String = n => if (n % 2 == 0) "par" else "impar"

    val udfParOImpar = udf(ParOImpar)

    val nuevaColumna = numeros.withColumn("par/impar", udfParOImpar(numeros("Edad")))

    nuevaColumna.show()
    nuevaColumna
  }



  /**Ejercicio 3: Joins y agregaciones
   Pregunta: Dado dos DataFrames,
   uno con información de estudiantes (id, nombre)
   y otro con calificaciones (id_estudiante, asignatura, calificacion),
   realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
   */


  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {

    val unionTablas = estudiantes.join(calificaciones, estudiantes("id") === calificaciones("id_estudiante"))

    val promedioCalificaciones = unionTablas
      .groupBy("id", "Nombre")
      .agg(avg("Calificacion")
        .alias("Promedio"))

    unionTablas.show()
    promedioCalificaciones.show()
    promedioCalificaciones
  }




  /**Ejercicio 4: Uso de RDDs
   Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra. */

  val lista = List("rosa","margarita","tulipan","rosa","rosa", "margarita", "tulipan", "margarita")

  def ejercicio4(palabras: List[String])(implicit spark:SparkSession): RDD[(String, Int)] = {

    val rdd = spark.sparkContext.parallelize(palabras)

    val contar = rdd.map(a => (a, 1)).reduceByKey(_ + _)

    contar
  }



  /**
   Ejercicio 5: Procesamiento de archivos
   Pregunta: Carga un archivo CSV que contenga información sobre
   ventas (id_venta, id_producto, cantidad, precio_unitario)
   y calcula el ingreso total (cantidad * precio_unitario) por producto.
   */

  def ejercicio5(ventas: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val totalIngresos = ventas.withColumn("ingreso", col("cantidad") * col("precio_unitario"))

    val ordenIngresos = {
      totalIngresos
        .groupBy("id_producto")
        .agg(sum("ingreso").alias("ingreso_total"))
    }

    ordenIngresos.show()
    ordenIngresos

  }

}
