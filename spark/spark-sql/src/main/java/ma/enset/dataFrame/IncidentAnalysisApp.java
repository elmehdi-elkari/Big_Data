package ma.enset.dataFrame;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.year;

public class IncidentAnalysisApp {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("Tp Dataframe")
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> df = sparkSession.read()
                .option("header",true)
                //.option("multiline",true) pour json syntaxe
                .csv("spark-sql/src/main/resources/incidents.csv");

        df.printSchema();
        df.show();

        // nombre d'incidence par service : SparkSql syntaxe
        df.groupBy("Service").count().show();

        // nombre d'incidence par service : SQL
        df.createOrReplaceTempView("incidents_table");
        sparkSession.sql("SELECT Service, COUNT(*) as IncidentCount FROM incidents_table GROUP BY Service").show();
        /*
                    * // Chargement des tables à partir de la base de données
            Dataset<Row> table1 = sparkSession.sql("SELECT * FROM table1");
            Dataset<Row> table2 = sparkSession.sql("SELECT * FROM table2");

            // Spécifiez les colonnes de jointure
            String joinCondition = "table1.common_column = table2.common_column";

            // Effectuer la jointure en utilisant SQL
            Dataset<Row> result = table1.join(table2, joinCondition);

            // Afficher les résultats
            result.show();

        * */


        df.groupBy(year(col("Date")).alias("year")).count().limit(2).show();

    }
}
