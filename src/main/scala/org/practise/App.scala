package org.practise

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ${prathambongale}
 */
object App {
  
  def main(args : Array[String]) {

    val sc =  new SparkContext(new SparkConf())

    val rdd = MongoSpark.load(sc)

    println("mongo record count : " + rdd.count)
    println("mongo list : " + rdd.collect().foreach(Document=>Unit))
    println("mongo first record : " + rdd.first().toJson())
    // Print All MongoDB collection record
    println("Collection Record : ")
    rdd.foreach(x => println(x))
    
    // Print Schema
    rdd.toDF().printSchema()

    // DataFrames can be saved as Parquet files, maintaining the schema information
    rdd.toDF().write.parquet("people.parquet")

    // Read in the parquet file created above
    // Parquet files are self-describing so the schema is preserved
    // The result of loading a Parquet file is also a DataFrame
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val parquetFileDF = sqlContext.read.parquet("people.parquet")

    // Parquet files can also be used to create a temporary view and then used in SQL statements
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = sqlContext.sql("SELECT * FROM parquetFile")
    //namesDF.map(attributes => "Name: " + attributes(0)).show()
    namesDF.show()
  }
}
