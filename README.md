# scala-test
This is sample project to test scala spark and mongodb connection


# Maven Command
mvn -DskipTests clean package


# Command to run this project:
spark-submit --class "org.practise.App" --master local target/scala-test-1.0-SNAPSHOT-jar-with-dependencies.jar