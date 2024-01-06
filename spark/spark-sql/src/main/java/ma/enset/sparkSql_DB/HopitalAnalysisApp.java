package ma.enset.sparkSql_DB;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class HopitalAnalysisApp {

    public static void main(String[] args) {

        SparkSession ss = SparkSession.builder().appName("MySQL").master("local[*]").getOrCreate();

        Map<String, String> option = new HashMap<>();
        option.put("driver", "com.mysql.jdbc.Driver");
        option.put("url", "jdbc:mysql://localhost:3306/db_hopital");
        option.put("user","root");
        option.put("password","");

        Dataset<Row> dfConcultation = ss.read().format("jdbc")
                .options(option)
                //.option("table","table_name")
                .option("query","select * from consultations")
                .load();

        Dataset<Row> dfmedecine = ss.read().format("jdbc")
                .options(option)
                //.option("table","table_name")
                .option("query","select * from medecins")
                .load();

        Dataset<Row> dfPatient = ss.read().format("jdbc")
                .options(option)
                //.option("table","table_name")
                .option("query","select * from patients")
                .load();

        //consultation by day count
        dfConcultation.groupBy(col("DATE_CONSULTATION").alias("day")).count().show();

        //consultation by medecin count
        dfmedecine
                .join(dfConcultation, dfmedecine.col("ID").equalTo(dfConcultation.col("ID_MEDECIN")))
                .groupBy(dfmedecine.col("ID"),dfmedecine.col("NOM"), dfmedecine.col("PRENOM")).count()
                .select(col("NOM"),col("PRENOM"),col("count").as("nbr"))
                .show();

        /*V2
            // q2
            Dataset<Row> dfConsultations = df1.groupBy(col("id_medecin")).count();
            Dataset<Row> dfMedicins = df2.select("id","nom","prenom");

            Dataset<Row> joinedDF = dfMedicins
                    .join(dfConsultations, dfMedicins.col("id").equalTo(dfConsultations.col("id_medecin")), "inner")
                    .select(dfMedicins.col("nom"), dfMedicins.col("prenom"), dfConsultations.col("count").alias("NOMBRE DE CONSULTATION"))
                    .orderBy(col("NOMBRE DE CONSULTATION").desc());

            joinedDF.withColumnRenamed("nom","NOM");
            joinedDF.withColumnRenamed("prenom","PRENOM");

            joinedDF.show();
        */


        //patient by medicine count

        Dataset<Row> joinedMedecinConsult = dfmedecine
                .join(dfConcultation,dfmedecine.col("ID").equalTo(dfConcultation.col("ID_MEDECIN")));
        Dataset<Row> joinedMedecinConsultPat = joinedMedecinConsult
                .join(dfPatient,dfPatient.col("ID").equalTo(joinedMedecinConsult.col("ID_PATIENT")));

        joinedMedecinConsultPat
                .groupBy(dfmedecine.col("NOM"),dfPatient.col("NOM"))
                .count()
                .show();



    }

}
