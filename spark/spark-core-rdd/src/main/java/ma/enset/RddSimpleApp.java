package ma.enset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class RddSimpleApp {
    public static void main(String[] args) {

        //Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
        //Logger.getLogger("ma.enset").setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf().setAppName("LearningRDD").setMaster("local[*]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<Double> rdd1 = javaSparkContext.parallelize(Arrays.asList(12.3,45.99,21.23,34.004));
        JavaRDD<Long> rdd2 = rdd1.map(d->d.longValue());

        rdd2.collect().forEach(System.out::println);

    }
}