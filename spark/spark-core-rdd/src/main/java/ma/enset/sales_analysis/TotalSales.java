package ma.enset.sales_analysis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Date;

public class TotalSales {

    public static void main(String[] args) {

        SparkConf conf=new SparkConf().setAppName("Sales Counter").setMaster("local[1]");
        JavaSparkContext sc=new JavaSparkContext(conf);

        JavaRDD<String> rddLines = sc.textFile("spark-core-rdd/src/main/resources/sales.csv");

        /*//total sales By city
        JavaPairRDD<String,Double> citySale = rddLines.mapToPair(x->new Tuple2<String,Double>(x.split(",")[1],Double.valueOf(x.split(",")[3])));
        JavaPairRDD<String,Double> cityTotalSale = citySale.reduceByKey((a,b) -> a+b);
        cityTotalSale.foreach(v-> System.out.println(v._1+": "+v._2));

        //total sales By year
        JavaPairRDD<String,Double> dateSale = rddLines.mapToPair(x->new Tuple2<String,Double>(x.split(",")[0].split("-")[0] ,Double.valueOf(x.split(",")[3])));
        JavaPairRDD<String,Double> dateTotalSale = dateSale.reduceByKey((a,b) -> a+b);
        dateTotalSale.foreach(v-> System.out.println(v._1+": "+v._2));*/

        //total sales By city and year
        JavaPairRDD<String,Double> cityDateSale = rddLines.mapToPair( x->new Tuple2<String,Double>(x.split(",")[1]+" "+x.split(",")[0].split("-")[0] ,Double.valueOf(x.split(",")[3])));
        JavaPairRDD<String,Double> dateCityTotalSale = cityDateSale.reduceByKey((a,b) -> a+b);

        dateCityTotalSale.foreach(v-> System.out.println(v._1+": "+v._2));



    }

}
