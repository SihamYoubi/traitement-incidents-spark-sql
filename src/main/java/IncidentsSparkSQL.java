import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.*;

public class IncidentsSparkSQL {
    public static void main(String[] args) {
        // 1. Créer une session Spark
        SparkSession spark = SparkSession.builder()
                .appName("TP Spark SQL - Incidents")
                .master("local[*]")
                .getOrCreate();

        // 2. Lire le fichier CSV
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
               .csv("incidents.csv");

        df.printSchema();

        // 3. Afficher le nombre d’incidents par service
        System.out.println("=== Nombre d’incidents par service ===");
        df.groupBy("service")
                .agg(count("*").alias("Nombre_Incidents"))
                .orderBy(desc("Nombre_Incidents"))
                .show();

        // 4. Extraire l'année depuis la date
        Dataset<Row> dfAvecAnnee = df.withColumn("annee", year(col("date")));

        // 5. Afficher les deux années avec le plus d’incidents
        System.out.println("=== Deux années avec le plus d’incidents ===");
        dfAvecAnnee.groupBy("annee")
                .agg(count("*").alias("Total_Incidents"))
                .orderBy(desc("Total_Incidents"))
                .limit(2)
                .show();

        spark.stop();
    }
}
