package com.sparksql;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;


/**
 * Created by heena
 *
 * 1) This class reads json file to dataset.
 * 2) Prints schema of json file and write proccessed data to another file.
 */
public class SparkSQLExample {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Spark SQL examples")
                .master("local")
                .getOrCreate();

        String jsonP="/Users/heena.madan/Documents/code-data/spark/apache-spark-sql-examples-master/data.json";
        Dataset<Row> data = spark.read().json(jsonP);

        //parquet files- https:docs.databricks.com/spark/latest/data-sources/read-parquet.html
        Dataset<Row> usersDF = spark.read().load("/Users/heena.madan/Documents/code-data/spark/apache-spark-sql-examples-master/users.parquet");

        usersDF.show();

        data.show();

        data.printSchema();

        Dataset<Row> fnameDS = data.select("fname");

        fnameDS.show();

        Dataset<Row> nameDS = data.select(col("fname"), col("lname"));

        nameDS.show();

        Dataset<Row> genderDS = data.groupBy("gender").count();

        genderDS.show();

        Dataset<Row> maleGenederDS = data.filter(col("gender").eqNullSafe("Male"));
        System.out.println("male data---");
        maleGenederDS.show();

        Dataset<Row> femaleDS = data.filter(col("gender").eqNullSafe("Female"));
        System.out.println("female data---");
        femaleDS.show();

        Dataset<String> fullNameDS = data.map(
                (MapFunction<Row, String>) row -> row.getAs("fname").toString() + " " +
                        row.getAs("lname").toString(),
                Encoders.STRING());
        fullNameDS.show();

        Dataset<String> fullname = data.map((MapFunction<Row,String>) row-> row.getAs("fname").toString() + "" + row.getAs("lname").toString(), Encoders.STRING());

        Dataset<Person> peopleDS = data.as(Encoders.bean(Person.class));

        peopleDS.show();

        //write to another json file
        String jsonPath = "/Users/heena.madan/Documents/code-data/spark/apache-spark-sql-examples-master/data1.json";
        Encoder<Person> employeeEncoder = Encoders.bean(Person.class);
        Dataset<Person> ds = spark.read().json("/Users/heena.madan/Documents/code-data/spark/apache-spark-sql-examples-master/data.json").as(employeeEncoder);

        fullNameDS.write().json("/Users/heena.madan/Documents/code-data/spark/apache-spark-sql-examples-master/fullname.json");
        ds.write().json(jsonPath);

        spark.stop();
    }
}
