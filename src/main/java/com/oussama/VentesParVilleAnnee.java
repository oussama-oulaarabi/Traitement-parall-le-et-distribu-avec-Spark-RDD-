package com.oussama;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

public class VentesParVilleAnnee {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("VentesParVilleEtAnnee").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lignes = sc.textFile("ventes.txt");

        // (ville, année) => prix
        JavaPairRDD<Tuple2<String, String>, Double> villeAnneePrix = lignes.mapToPair(line -> {
            String[] parts = line.split(" ");
            String date = parts[0]; // e.g., 2023-01-15
            String annee = date.split("-")[0];
            String ville = parts[1];
            Double prix = Double.parseDouble(parts[3]);
            return new Tuple2<>(new Tuple2<>(ville, annee), prix);
        });

        // Somme des prix
        JavaPairRDD<Tuple2<String, String>, Double> totalParVilleAnnee = villeAnneePrix.reduceByKey(Double::sum);

        totalParVilleAnnee.collect().forEach(tuple -> {
            Tuple2<String, String> cle = tuple._1();
            Double total = tuple._2();
            System.out.println(cle._1() + " (" + cle._2() + ") : " + total);
        });

        sc.close();
    }
}
