package org.practise

/**
 * @author ${prathambongale}
 */
object App {
  
  //def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {

    //println( "Hello World!" )
    //println("concat arguments = " + foo(args))

    import org.apache.spark.sql.SparkSession
    
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("App")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/scala_spark_db.test")
      //.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/scala_spark_db.test")
      .getOrCreate()

    import com.mongodb.spark.MongoSpark

    val sc = spark.sparkContext

    val rdd = MongoSpark.load(sc)
    val df = MongoSpark.load(spark)

    df.printSchema() // Prints DataFrame schema
    println("mongo record count : " + rdd.count)
    println("mongo list : " + rdd.collect().foreach(Document=>Unit))
    println("mongo first record : " + rdd.first().toJson())

    println("Mongo records : " + df.show())

    println("Collection Record : ")
    rdd.foreach(x => println(x))
  
    // Encoders for most common types are automatically provided by importing spark.implicits._
    import spark.implicits._
    //val peopleDF = spark.read.json("examples/src/main/resources/people.json")

    // DataFrames can be saved as Parquet files, maintaining the schema information
    df.write.parquet("people.parquet")

    // Read in the parquet file created above
    // Parquet files are self-describing so the schema is preserved
    // The result of loading a Parquet file is also a DataFrame
    val parquetFileDF = spark.read.parquet("people.parquet")

    // Parquet files can also be used to create a temporary view and then used in SQL statements
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT name FROM parquetFile")
    namesDF.map(attributes => "Name: " + attributes(0)).show()

  }

}
