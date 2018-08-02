package com.sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;

public class ConvertCollectionToDataset {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .master("local[*]")
                .appName("Example")
                .getOrCreate();


        Encoder<TestModel> personEncoder = Encoders.bean(TestModel.class);
        Dataset<TestModel> javaBeanDS = spark.createDataset(
                Collections.singletonList(new TestModel("a", "b", "c")),
                personEncoder
        );
        javaBeanDS.show();

    }
}
