package com.oussama;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

public class VentesParVille {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TotalVentesParVille").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Lire le fichier texte
        JavaRDD<String> lignes = sc.textFile("ventes.txt");

        // Créer des paires (ville, prix)
        JavaPairRDD<String, Double> villePrix = lignes.mapToPair(line -> {
            String[] parts = line.split(" ");
            String ville = parts[1];
            Double prix = Double.parseDouble(parts[3]);
            return new Tuple2<>(ville, prix);
        });

        // Réduire par ville
        JavaPairRDD<String, Double> ventesParVille = villePrix.reduceByKey(Double::sum);

        // Afficher le résultat
        ventesParVille.collect().forEach(tuple ->
                System.out.println(tuple._1() + " : " + tuple._2())
        );

        sc.close();
    }
}
