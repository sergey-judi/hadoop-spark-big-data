package com.big.data.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.List;

import static com.big.data.util.InvertedIndexUtils.formatMappedValue;
import static com.big.data.util.InvertedIndexUtils.tokenizeRow;
import static com.big.data.util.InvertedIndexUtils.wrapText;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  protected void map(LongWritable key, Text row, Context context) throws IOException, InterruptedException {
    FileSplit inputSplit = (FileSplit) context.getInputSplit();
    String fileName = inputSplit.getPath().getName();

    long byteOffset = key.get();

    List<String> tokens = tokenizeRow(row.toString());

    for (String token : tokens) {
      context.write(
          wrapText(token),
          wrapText(formatMappedValue(fileName, byteOffset))
      );
    }

  }

}
