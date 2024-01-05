package ma.enset.sales_analysis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public class CountSalesByCity {

    public static void main(String[] args) {

        SparkConf conf=new SparkConf().setAppName("Sales Counter").setMaster("local[1]");
        JavaSparkContext sc=new JavaSparkContext(conf);

        JavaRDD<String> rddLines = sc.textFile("spark-core-rdd/src/main/resources/sales.csv");
        JavaRDD<String> rddCity = rddLines.map(line -> line.split(",")[1]);
        JavaPairRDD<String,Integer> pairRDD = rddCity.mapToPair(city -> new Tuple2<String,Integer>(city,1)); //PairFunction !

        JavaPairRDD<String,Integer> cityCountRdd = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        //Solution 2: JavaPairRDD<String,Integer> wordCount=pairRDDCities.reduceByKey((a,b)-> a+b);

        cityCountRdd.foreach(c->System.out.println(c._1+": "+c._2));





    }

}
