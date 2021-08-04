# scala-test
This is sample project to test scala spark and mongodb connection


# Maven Command
mvn -DskipTests clean package


# Command to run this project:
spark-submit --name "MongoDB Spark Scala Test" --conf spark.mongodb.input.uri=mongodb://127.0.0.1:27017/user.userdetails --class "org.practise.App" --master local[*] target/scala-test-1.0-SNAPSHOT-jar-with-dependencies.jar "hdfs://127.0.0.1:9000/output/mongodb.parquet"