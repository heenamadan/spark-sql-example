package com.sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class FilterDataset {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Dataset-Basic")
                .master("local[4]")
                .getOrCreate();

        List<Integer> data = Arrays.asList(10, 11, 12, 13, 14, 15);
        Dataset<Integer> ds = sparkSession.createDataset(data, Encoders.INT());

        System.out.println("*** only one column, and it always has the same name");
        ds.printSchema();

        ds.show();


        Dataset<Integer> ds2 = ds.filter((Integer value) -> value > 12);

        ds2.show();
    }
}
