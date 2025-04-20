package examen

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import utils.TestInit
import examen._

class examenTest extends TestInit {

  val sc = spark.sparkContext


  "ejercicio 1" should "crear un dataframe" in {

    val schema = StructType(Seq(
      StructField("Nombre", StringType, nullable = false),
      StructField("Edad", IntegerType, nullable = false),
      StructField("Calificacion", DoubleType, nullable = false)
    ))

    val estudiantes_1 = Seq(
      Row("María", 20, 9.1),
      Row("Juan", 22, 7.5),
      Row("Lucía", 19, 8.7),
      Row("Pedro", 21, 6.3),
      Row("Sofía", 23, 9.5)
    )
    val estudiantes = spark.createDataFrame(spark.sparkContext.parallelize(estudiantes_1), schema)
    ejercicio1(estudiantes)
  }

  "ejercicio2" should "determinar si los años de los estudiantes son pares o impares" in {
    val estudiantes = Seq(
      Row("María", 20, 9.1),
      Row("Juan", 22, 7.5),
      Row("Lucía", 19, 8.7),
      Row("Pedro", 21, 6.3),
      Row("Sofía", 23, 9.5)
    )
    val schema = StructType(Seq(
      StructField("Nombre", StringType, nullable = false),
      StructField("Edad", IntegerType, nullable = false),
      StructField("Calificacion", DoubleType, nullable = false)
    ))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(estudiantes), schema)

    ejercicio2(df)
  }


  "ejercicio3" should "realizar un join y calcular el promedio de calificaciones por estudiante" in {
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

    val estudiantes = spark.createDataFrame(spark.sparkContext.parallelize(datos), datosSchema)

    val informacionAcademica = Seq(
      Row(1, "Matemáticas", 5.5),
      Row(1, "Inglés", 7.4),
      Row(1, "Geografía", 5.3),
      Row(2, "Química", 5.5),
      Row(2, "Inglés", 9.1),
      Row(3, "Lengua", 7.2),
      Row(4, "Matemáticas", 8.5),
      Row(4, "Física", 3.4),
      Row(5, "Historia", 6.8)
    )
    val informacionAcademicaSchema = StructType(Seq(
      StructField("id_estudiante", IntegerType, nullable = false),
      StructField("Asignatura", StringType, nullable = false),
      StructField("Calificacion", DoubleType, nullable = false),
    ))

    val calificaciones = spark.createDataFrame(spark.sparkContext.parallelize(informacionAcademica), informacionAcademicaSchema)
    ejercicio3(estudiantes, calificaciones)
  }


  "ejercicio4" should "crear un RDD a partir de una lista y contar la cantidad de ocurrencia de cada palabra" in {

    val palabras = List("rosa", "margarita", "tulipan", "rosa", "rosa", "margarita", "tulipan", "margarita")
    ejercicio4(lista)
    ejercicio4(lista).collect().foreach(println)

  }


  "ejercicio5" should "calcular el ingreso total por producto" in {

    val dfVentas = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("C:\\Users\\noeli\\Desktop\\prueba_processing\\src\\test\\resources\\examen\\ventas.csv")

    ejercicio5(dfVentas)
  }

}
