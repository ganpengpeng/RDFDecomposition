package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class App {
    public static void main(String[] args) {
        System.out.println("Hello World!");
        SparkConf conf = new SparkConf().setAppName("mySpark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("file:///home/ganpeng/spark/testfile");
//        for (String line : lines.collect()) {
//            System.out.println(line);
//        }
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
//        for (String word : words.collect()) {
//            System.out.println(word);
//        }
        JavaPairRDD<String, Integer> pairs = words.mapToPair((s -> new Tuple2<>(s, 1)));
//        for (Tuple2<String, Integer> pair : pairs.collect()) {
//            System.out.println(pair._1 + ' ' + pair._2);
//        }
        JavaPairRDD<String, Integer> wordsCount = pairs.reduceByKey(((v1, v2) -> v1 + v2));
        wordsCount.foreach((Tuple2<String, Integer> pair) -> System.out.println(pair._1 + ':' + pair._2));
        sc.close();
    }
}

