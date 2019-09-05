package com.elminda.recruitment.sparkwordcount;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;


@SpringBootApplication
public class SparkWordCountApplication implements CommandLineRunner {

    public static final String INPUT_FILE_PATH = "/data/alice-in-wonderland.txt";
    private static Logger log = LoggerFactory.getLogger(SparkWordCountApplication.class);

    @Autowired
    JavaSparkContext sc;

    public static void main(String[] args) {
        SpringApplication.run(SparkWordCountApplication.class, args);
    }

    @Override
    public void run(String... args) {
        final String parentDirectory = getParentDirectory();

        //this step reads the file from disk. the result would be an rdd entry per line in the file
        final JavaRDD<String> inputFile = sc.textFile(parentDirectory + INPUT_FILE_PATH);

        //this step splits the words by space and then lowers their casing and filters any non alphanumeric values from them
        final JavaRDD<String> words = getCleanWordsFromFile(inputFile);

        //this step appends '1' to each word and makes a pair rdd to aggregate it in the future steps
        final JavaPairRDD<String, Integer> wordWithCounter = getWordWithCounter(words);

        //this step aggregates the words. the actual count is in this method
        final JavaPairRDD<String, Integer> map = aggregateWordsCount(wordWithCounter);

        //this step prepares the data for printing - collects it to a single partition
        //and transforms the pair to the desired string format
        final JavaRDD<String> formattedRdd = formatForOutput(map);

        //this step prints the results
        formattedRdd.saveAsTextFile(parentDirectory + "/data/results");
    }

    private JavaRDD<String> getCleanWordsFromFile(JavaRDD<String> inputFile) {
        return inputFile
                .flatMap(f -> Arrays.asList(f.split(" ")).iterator())
                .map(word -> word.toLowerCase().replaceAll("[^a-z]", ""))
                .filter(f -> !f.isEmpty());
    }

    private JavaPairRDD<String, Integer> getWordWithCounter(JavaRDD<String> words) {
        return words.mapToPair(word -> new Tuple2<>(word, 1));
    }

    private JavaPairRDD<String, Integer> aggregateWordsCount(JavaPairRDD<String, Integer> wordWithCounter) {
        return wordWithCounter.reduceByKey(Integer::sum);
    }

    private JavaRDD<String> formatForOutput(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) {
        return stringIntegerJavaPairRDD.repartition(1).sortByKey().map(keyValue -> keyValue._1() + " " + keyValue._2());
    }

    private String getParentDirectory() {
        try {
            return new File(".").getCanonicalPath();
        } catch (IOException e) {
            return "";
        }
    }
}
