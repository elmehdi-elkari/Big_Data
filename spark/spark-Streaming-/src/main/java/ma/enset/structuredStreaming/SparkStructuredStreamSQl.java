package ma.enset.structuredStreaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class SparkStructuredStreamSQl {

    /*public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession ss = SparkSession
                .builder()
                .appName("Company Incidents")
                .master("local[*]")
                .getOrCreate();

        StructType schema = new StructType(
                new StructField[]{
                        new StructField("id", DataTypes.LongType, false, Metadata.empty()),
                        new StructField("titre", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("description", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("service", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("date", DataTypes.StringType, false, Metadata.empty()),
                }
        );

        Dataset<Row> lines = ss.readStream()
                .option("header", true)
                .schema(schema)
                .csv("spark-Streaming-/src/main/resources");

        StreamingQuery query = lines.writeStream()
                .outputMode("append")
                .format("console")
                .start();

        query.awaitTermination();
    }*/

    public static void main(String[] args) throws TimeoutException {
        // Set Spark log level to ERROR
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

        SparkSession spark = SparkSession.builder()
                .appName("HospitalIncidentStreaming")
                .master("local[2]")
                .getOrCreate();

        // Set application log level to INFO
        Logger.getLogger("ma.enset").setLevel(Level.INFO);

        // Define the schema for streaming
        String schema = "Id INT, titre STRING, description STRING, service STRING, date STRING";

        // Read streaming data from the specified directory
        Dataset<Row> streamingDF = spark.readStream()
                .schema(schema)
                .csv("spark-Streaming-/src/main/resources");

        // Task 1: Display the number of incidents per service continuously
        Dataset<Row> incidentsByService = streamingDF.groupBy("service")
                .agg(count("Id").alias("incident_count"));

        StreamingQuery query1 = incidentsByService.writeStream()
                .outputMode(OutputMode.Complete())
                .format("console")
                .start();

        // Task 2: Display the two years with the highest number of incidents continuously
        Dataset<Row> incidentsByYear = streamingDF
                .withColumn("year", substring(col("date"), 1, 4))
                .groupBy("Date")
                .agg(count("Id").alias("incident_count"))
                .orderBy(col("incident_count").desc())
                .limit(2);

        StreamingQuery query2 = incidentsByYear.writeStream()
                .outputMode(OutputMode.Complete())
                .format("console")
                .start();

        // Wait for the streaming to finish
        try {
            query1.awaitTermination();
            query2.awaitTermination();
        } catch (StreamingQueryException e) {
            throw new RuntimeException(e);
        }

    }
}
