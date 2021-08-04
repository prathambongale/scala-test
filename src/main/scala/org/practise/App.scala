package org.practise

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession

/**
 * @author ${prathambongale}
 */
object App {
  
  def main(args : Array[String]) {

    // Logging
    val Logger = LoggerFactory.getLogger(getClass().getName)

    try {

      // Configuration
      val spark:SparkSession = SparkSession
        .builder()
        .getOrCreate()

      // MongoDB Connection
      val rdd = MongoSpark.load(spark)

      // Write data into Hadoop Subsystem
      rdd.toDF().write.mode("overwrite").parquet(args(0))
    
    } catch {
      case ex: Exception => {
        Logger.error("Exception has Occurred : ")
        ex.printStackTrace
      }
    }
  }
}
