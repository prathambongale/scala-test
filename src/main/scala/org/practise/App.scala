package org.practise

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {

    import org.apache.spark.sql.SparkSession

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("App")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/scala_spark_db.test")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/scala_spark_db.test")
      .getOrCreate()

    import com.mongodb.spark._
    import com.mongodb.spark.config._
    import org.bson.Document 

    println( "Hello World!" )
    println("concat arguments = " + foo(args))

    val df = MongoSpark.load(sparkSession)  // Uses the SparkSession
    df.printSchema()
    val readConfig = ReadConfig()
  
  }

}
