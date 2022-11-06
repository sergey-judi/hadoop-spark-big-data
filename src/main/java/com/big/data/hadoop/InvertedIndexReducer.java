package com.big.data.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.big.data.util.InvertedIndexUtils.wrapText;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    List<String> tokenInvertedIndex = new ArrayList<>();

    values.forEach(
        v -> tokenInvertedIndex.add(v.toString())
    );

    context.write(
        key,
        wrapText(tokenInvertedIndex.toString())
    );
  }

}
