package com.sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 *
 * This class adds new column to json file.
 * **/
public class AddNewColumnToDS {

    public static void main(String[] args) {
        // configure spark
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Example - Add a new Column to Dataset")
                .master("local[2]")
                .getOrCreate();

        String jsonPath = "/Users/heena.madan/Documents/code-data/spark/apache-spark-sql-examples-master/test.json";
        Dataset<Row> ds = spark.read().json(jsonPath);

        // dataset before adding new column
        ds.show();

        // add column to ds
        Dataset<Row> newDs = ds.withColumn("new_column_1", functions.lit(1));

        // print dataset after adding new column
        newDs.show();

        spark.stop();
    }
}

