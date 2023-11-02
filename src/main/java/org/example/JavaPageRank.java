package org.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.List;
import scala.collection.JavaConverters;
import scala.collection.convert.Wrappers.SeqWrapper;
import scala.collection.Seq;
import java.util.Collections;
import scala.collection.mutable.WrappedArray;
import scala.Tuple2;

public final class JavaPageRank {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: JavaPageRank <inputDirectory>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .getOrCreate();

        String inputDirectory = args[0];  // Input directory containing JSON files
        Dataset<Row> inputData = spark.read()
//                .option("mode", "DROPMALFORMED")
                .option("multiline", true) // Allow multiline JSON records
                .json(inputDirectory);

        inputData.printSchema();

        JavaRDD<Object> playlistsAndTracks = inputData.select("playlists.tracks").javaRDD().flatMap(row -> {
            return row.getList(0).iterator();
        });

        JavaRDD<String> playlists = inputData.select("playlists.name").javaRDD().flatMap(row -> {
            return row.getList(0).iterator();
        }).map(item -> (String)item);


        List<Object> first5Rows = playlistsAndTracks.take(1);
        List<String> firstRowPlaylists = playlists.take(1);

        // Print the content of the first 5 rows
        for (Object row : first5Rows) {
            System.out.println(row.getClass().getName());
            System.out.println(row);
        }

        for (Object row : firstRowPlaylists) {
            System.out.println("________________________");
            System.out.println(row);
        }


        spark.stop();
    }
}