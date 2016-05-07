package com.HW1.Problem1;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Problem1Reducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    NullWritable out = NullWritable.get();

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text t : values) {
            context.write(out, t);
        }
    }

}
