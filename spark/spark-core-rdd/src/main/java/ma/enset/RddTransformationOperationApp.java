package ma.enset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class RddTransformationOperationApp {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("LearningRDD").setMaster("local[1]");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<String> students = Arrays.asList("Mohammed", "Rokia", "Ahmed", "Youssef", "Hanane", "Hicham", "Mohammed", "Aya");
        JavaRDD<String> rdd1 = javaSparkContext.parallelize(students);

        // Flatmap
        JavaRDD<String> rdd2 = rdd1.flatMap(s -> Arrays.asList(s,"EL KARI").iterator());
        System.out.println("\n-------------- RDD2 --------------\n");
        rdd2.foreach(name-> System.out.print(name+", "));
        System.out.println("\n-------------- ---- --------------\n");

        //Filter
        JavaRDD<String> rdd3 = rdd2.filter(x -> !(x.startsWith("E")));
        System.out.println("\n-------------- RDD3 --------------\n");
        rdd3.foreach(name-> System.out.print(name+", "));
        System.out.println("\n-------------- ---- --------------\n");

        //Filter
        JavaRDD<String> rdd4 = rdd2.filter(x -> x.length() > 7);
        System.out.println("\n-------------- RDD4 --------------\n");
        rdd4.foreach(name-> System.out.print(name+", "));
        System.out.println("\n-------------- ---- --------------\n");

        //Union
        JavaRDD<String> rdd5 = rdd3.union(rdd4);
        System.out.println("\n-------------- RDD5 --------------\n");
        rdd5.foreach(name-> System.out.print(name+", "));
        System.out.println("\n-------------- ---- --------------\n");

        //MapToPair
        JavaPairRDD<Integer, String> rdd6 = rdd5.mapToPair(x -> new Tuple2<>(x.length(), x));
        System.out.println("\n-------------- RDD6 --------------\n");
        rdd6.foreach(name-> System.out.print(name+", "));
        System.out.println("\n-------------- ---- --------------\n");

        //Reduce By key
        JavaPairRDD<Integer, String> rdd7 = rdd6.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s+" "+s2;
            }
        });
        System.out.println("\n-------------- RDD7 --------------\n");
        rdd7.foreach(name-> System.out.print(name+", "));
        System.out.println("\n-------------- ---- --------------\n");
        
    }

}
