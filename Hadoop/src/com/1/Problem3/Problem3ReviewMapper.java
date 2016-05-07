package com.HW1.Problem3;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Problem3ReviewMapper extends Mapper<Object, Text, Text, DoubleWritable> {

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] line = value.toString().split("\\^");

        String bId = line[2];
        double rating = Double.parseDouble(line[3]);

        context.write(new Text(bId), new DoubleWritable(rating));
    }

}
