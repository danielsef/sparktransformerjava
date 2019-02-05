package com.streamsets.spark.java;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.spark.api.SparkTransformer;
import com.streamsets.pipeline.spark.api.TransformResult;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;


public class CustomTransformer extends SparkTransformer implements Serializable {
    private transient JavaSparkContext javaSparkContext;
    private int kudu;

    @Override
    public void init(JavaSparkContext javaSparkContext, List<String> params) {
        this.javaSparkContext = javaSparkContext;
        this.kudu = new Sample().getKuduResult();
    }

    @Override
    public TransformResult transform(JavaRDD<Record> records) {
        // Create an empty errors JavaPairRDD
        JavaRDD<Tuple2<Record,String>> emptyRDD = javaSparkContext.emptyRDD();
        JavaPairRDD<Record, String> errors = JavaPairRDD.fromJavaRDD(emptyRDD);

        // Apply a map to the incoming records
        JavaRDD<Record> result = records.map(new Function<Record, Record>() {
            @Override
            public Record call(Record record) throws Exception {
                // Just return the incoming record
                return record;
            }
        });
        return new TransformResult(result, errors);
    }
}