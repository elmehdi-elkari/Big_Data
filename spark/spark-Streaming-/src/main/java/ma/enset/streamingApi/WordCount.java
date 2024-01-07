package ma.enset.streamingApi;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("Spark Streaming").setMaster("local[*]");

        // Durations of micro batchs
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(8));

        // Stream source
        JavaReceiverInputDStream<String> dStreamLines = sc.socketTextStream("localhost", 8080);
        JavaDStream<String> dStreamWords = dStreamLines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> dPairStream = dStreamWords.mapToPair(w -> new Tuple2<>(w, 1));
        JavaPairDStream<String, Integer> dPairStreamWordCount = dPairStream.reduceByKey((a, b) -> a+b);


        dPairStreamWordCount.print();

        sc.start();
        sc.awaitTermination();

    }

}
