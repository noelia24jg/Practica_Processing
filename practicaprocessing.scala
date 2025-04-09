package job.examen


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.functions.udf

object examen {

  implicit val spark: SparkSession = SparkSession.builder()
    .appName("Examen")
    .master("local[*]")
    .getOrCreate()




  /** Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   * Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
   * estudiantes (nombre, edad, calificación).
   * Realiza las siguientes operaciones:
   *
   * Muestra el esquema del DataFrame.
   * Filtra los estudiantes con una calificación mayor a 8.
   * Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */

  val estudiantes = Seq(
    Row("Rosa", 27, 4),
    Row("Federico", 20, 6.3),
    Row("Marcelo", 18, 5.4),
    Row("Laura", 25, 9.6),
    Row("Carolina", 30, 8.2)
  )

  val schema = StructType(Seq(
    StructField("Nombre", StringType, nullable = false),
    StructField("Edad", IntegerType, nullable = false),
    StructField("Calificacion", DoubleType, nullable = false))
  )

  val df = spark.createDataFrame(spark.sparkContext.parallelize(estudiantes), schema)


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

  def ejercicio2(df: DataFrame): DataFrame = {

    val ParOImpar: Int => String = n => if (n % 2 == 0) "par" else "impar"

    val udfParOImpar = udf(ParOImpar)

    val nuevaColumna = df.withColumn("par/impar", udfParOImpar(df("Edad")))

    nuevaColumna.show()
    nuevaColumna
  }




  /**Ejercicio 3: Joins y agregaciones
   Pregunta: Dado dos DataFrames,
   uno con información de estudiantes (id, nombre)
   y otro con calificaciones (id_estudiante, asignatura, calificacion),
   realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
   */

  val datos = Seq(
    Row(1, "Rosa"),
    Row(2, "Federico"),
    Row(3, "Marcelo"),
    Row(4, "Laura"),
    Row(5, "Carolina")
  )

  val datosSchema = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("Nombre", StringType, nullable = false)
  ))

  val df3 = spark.createDataFrame(spark.sparkContext.parallelize(datos), datosSchema)

  val informacionAcademica = Seq(
    Row(1, "Matematicas", 5.5),
    Row(1, "Ingles", 7.4),
    Row(1, "Geografia", 5.3),
    Row(2, "Quimica", 5.5),
    Row(2, "Ingles", 9.1),
    Row(3, "Lengua", 7.2),
    Row(4, "Matematicas", 8.5),
    Row(4, "Fisica", 3.4),
    Row(5, "Historia", 6.8)
  )
  val informacionAcademicaSchema = StructType(Seq(
    StructField("id_estudiante", IntegerType, nullable = false),
    StructField("Asignatura", StringType, nullable = false),
    StructField("Calificacion", DoubleType, nullable = false),
  ))

  val df3_1 = spark.createDataFrame(spark.sparkContext.parallelize(informacionAcademica), informacionAcademicaSchema)


  def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {

    val unionTablas = df3.join(df3_1, df3("id") === df3_1("id_estudiante"))

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

  val dfVentas = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("C:\\Users\\noeli\\Desktop\\prueba_processing\\examen\\ventas.csv")

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


