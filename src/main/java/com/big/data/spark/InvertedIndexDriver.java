package com.big.data.spark;

import com.big.data.util.InvertedIndexUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class InvertedIndexDriver {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("inverted-index-using-spark");

    String inputPath = args[0];
    String outputPath = args[1];

    try (JavaSparkContext sparkContext = new JavaSparkContext(sparkConf)) {
      sparkContext.wholeTextFiles(inputPath)
          .flatMap(InvertedIndexDriver::tokenizeFileContent)
          .groupBy(Pair::getKey)
          .map(InvertedIndexDriver::reduceByToken)
          .saveAsTextFile(outputPath);
    }

  }

  private static Iterator<Pair<String, String>> tokenizeFileContent(Tuple2<String, String> tuple) {
    String filePath = tuple._1();
    String fileContent = tuple._2();

    String fileName = Path.of(filePath).getFileName().toString();

    return Arrays.stream(fileContent.split(InvertedIndexUtils.ROW_DELIMITER))
        .map(InvertedIndexUtils::tokenizeRow)
        .flatMap(row -> row.stream().map(token -> Pair.of(token, fileName)))
        .iterator();
  }

  private static String reduceByToken(Tuple2<String, Iterable<Pair<String, String>>> tuple) {
    String token = tuple._1();
    Iterable<Pair<String, String>> pairs = tuple._2();

    Map<String, Integer> invertedIndex = StreamSupport.stream(pairs.spliterator(), false)
        .collect(Collectors.toMap(Pair::getRight, v -> 1, Integer::sum));

    return String.format("%s %s", token, invertedIndex.entrySet());
  }

}
