package com.tolgakumbul

import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MongoReadSpark {
    //mvn package exec:java -Dexec.mainClass=com.tolgakumbul.MongoReadSpark

    /*
    VM Options
    -Dspark.test.name=TOlgaKUmbulll
    -Dspark.master=local[*]
    -Dspark.app.name=MyApp
    -Dspark.mongodb.read.connection.uri=mongodb://127.0.0.1
    -Dspark.mongodb.write.connection.uri=mongodb://127.0.0.1
    --add-exports
    java.base/sun.nio.ch=ALL-UNNAMED
     */
    def main(args: Array[String]) {

        val conf = new SparkConf()
        val spark = SparkSession.builder().config(conf).getOrCreate()
        spark.sparkContext.hadoopConfiguration.setClass("fs.file.impl", classOf[BareLocalFileSystem], classOf[FileSystem])


        val testFromConf = spark.conf.get("spark.test.name")

        println("Printing spark session variables:")
        println("Succesfully fetch data from config " + testFromConf)
        println("App Name: " + spark.sparkContext.appName)
        println("Deployment Mode: " + spark.sparkContext.deployMode)
        println("Master: " + spark.sparkContext.master)

        val read = spark.conf.get("spark.mongodb.read.connection.uri")
        val write = spark.conf.get("spark.mongodb.write.connection.uri")

        println("Mongodb configs " + read + " " + write)

        val pipeline = "{title:/^The.*/}"

        val df = spark.read.format("mongodb")
          .option("database", "MyTestDb")
          .option("collection", "TestingSpark")
          .load()
          //.drop("thumbnailUrl")

        println("Showing DataFrame schema")
        df.printSchema()

        println("Showing Data in MongoDb")
        df.show()

        val value = df.filter(df("title").rlike("^The.*"))
        val sortedVal = value.orderBy(col("publishedDate").desc)
        println("Showing filtered data with the title field starting with \"The\" and sorted by \"publishedDate\" desc")
        sortedVal.show()

        /*
        **Apache Spark RDD**

        RDD is a fundamental structure of data and a fixed collection of data that calculates on a cluster’s different nodes.
        It enables a developer to process computations on large clusters inside the memory in a resilient manner.
        This way, it increases the efficiency of the task.

        The Resilient Distributed Dataset or RDD is Spark’s primary programming abstraction.
        It represents a collection of elements that is: immutable, resilient, and distributed.
        An RDD encapsulates a large dataset, Spark will automatically distribute the data contained in RDDs across our cluster and parallelize the operations we perform on them.

        **Apache Spark Dataframes**

        As compared to RDD, data in the Dataframe is arranged into named columns.
        It is a fixed distributed data collection that enables Spark developers to implement a structure on distributed data.
        This way, it allows abstraction at a higher level.

        DataFrames store data in a more efficient manner than RDDs, this is because they use the immutable, in-memory, resilient, distributed, and parallel capabilities of RDDs but they also apply a schema to the data.
        DataFrames also translate SQL code into optimized low-level RDD operations.
         */
        val rdd = df.rdd
        val rddFirst = rdd.first()
        println("Rdd first element : " + rddFirst)
        val rddJson = JsonConverterHelper.convertToJson(rddFirst)
        println("Rdd converted into Json via helper : " + rddJson)


        /*
        **Datasets**

        A dataset is a set of strongly-typed, structured data.
        They provide the familiar object-oriented programming style plus the benefits of type safety since datasets can check syntax and catch errors at compile time.
        Dataset is an extension of DataFrame.
        The goal of Spark Datasets is to provide an API that allows users to easily express transformations on object domains, while also providing the performance and robustness advantages of the Spark SQL execution engine
         */

        /*
         JavaRDD is a simple wrapper around RDD just to make calls from Java code more convenient.
         */
        val javaRdd = df.toJavaRDD
        val javaRddFirst = javaRdd.first()
        println("JavaRdd first element : " + javaRddFirst)

        val javaRddJson = JsonConverterHelper.convertToJson(javaRddFirst)
        println("JavaRdd converted into Json via helper : " + javaRddJson)


        /*
         * Writing mongodb df to csv
         */
        spark.time{
            df.withColumn("authors", col("authors").cast("string"))
              .withColumn("categories", col("categories").cast("string"))
              .write.option("header", "true")
              .format("com.databricks.spark.csv")
              .save("C:/Users/USER/Documents/SparkOutput/output.csv")
        }

        spark.stop()
    }
}