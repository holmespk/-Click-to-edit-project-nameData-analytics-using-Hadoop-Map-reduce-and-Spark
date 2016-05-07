package com.HW1.Problem2;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Problem2Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] line = value.toString().split("\\^");

        String bId = line[2];
        double rating = Double.parseDouble(line[3]);

        context.write(new Text(bId), new DoubleWritable(rating));
    }
}
