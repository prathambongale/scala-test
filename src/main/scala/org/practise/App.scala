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
  
  }

}
