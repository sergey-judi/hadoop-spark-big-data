package com.big.data.util;

import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class InvertedIndexUtils {

  public static final String ROW_DELIMITER = "\n";
  private static final String BLANK_STRING = "";
  private static final String MAPPED_VALUE_TEMPLATE = "%s@%s";
  private static final Pattern regexPattern = Pattern.compile("\\b\\w+\\b");

  private InvertedIndexUtils() {}

  public static List<String> tokenizeRow(String textRow) {
    List<String> tokens = new ArrayList<>();

    String reducedText = textRow
        .replaceAll("\\d+", BLANK_STRING)
        .replaceAll("[_,.]", BLANK_STRING);

    StringTokenizer tokenizedRow = new StringTokenizer(reducedText);

    while (tokenizedRow.hasMoreTokens()) {
      tokens.add(tokenizedRow.nextToken());
    }

    return tokens.stream()
        .filter(regexPattern.asMatchPredicate())
        .collect(Collectors.toList());
  }

  public static Text wrapText(String text) {
    return new Text(text);
  }

  public static String formatMappedValue(String fileName, long byteOffset) {
    return String.format(MAPPED_VALUE_TEMPLATE, fileName, byteOffset);
  }

}
