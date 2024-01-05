package ma.enset.climat_analysis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class ClimatAnalysts {

    public static void main(String[] args) {

        SparkConf conf=new SparkConf().setAppName("Climat Analysis").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);

        //download file from : https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/
        JavaRDD<String> rddLines = sc.textFile("spark-core-rdd/src/main/resources/1999.csv");

        JavaPairRDD<String, Double> rddT=rddLines.mapToPair(line -> {
            String[] split = line.split(",");
            return new Tuple2<>(split[2], Double.parseDouble(split[3]));
        });

        JavaPairRDD<String, Double> rddTMIN = rddT.filter(pair -> pair._1.contains("TMIN"));
        JavaPairRDD<String, Double> rddTMAX = rddT.filter(pair -> pair._1.contains("TMAX"));

        int TMINSize = rddTMIN.collect().size();
        int TMAXSize = rddTMAX.collect().size();

        JavaPairRDD<String, Double> rddSumTMIN = rddTMIN.reduceByKey((x,y) -> x+y);
        JavaPairRDD<String, Double> rddSumTMAX = rddTMAX.reduceByKey((x,y) -> x+y);

        rddSumTMIN.foreach(e -> System.out.println(e._1 + " " + e._2/TMINSize));
        rddSumTMIN.foreach(e -> System.out.println(e._1 + " " + e._2/TMAXSize));

        JavaPairRDD<String, Double> rddMeanTMIN = rddSumTMIN.mapValues(x -> (x / TMINSize));
        System.out.println(rddMeanTMIN.collect().get(0));
        JavaPairRDD<String, Double> rddMeanTMAX = rddSumTMAX.mapValues(x -> (x / TMAXSize));
        System.out.println(rddMeanTMAX.collect().get(0));



    }

}
