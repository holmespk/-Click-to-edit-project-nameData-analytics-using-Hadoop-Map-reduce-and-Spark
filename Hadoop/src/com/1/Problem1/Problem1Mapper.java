package com.HW1.Problem1;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Problem1Mapper extends Mapper<Object, Text, NullWritable, Text> {
    private Text businessId = new Text();
    private NullWritable out = NullWritable.get();

    @Override

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split("\\^");

        String id = line[0];
        businessId.set(id);

        if (line[1].contains("Palo Alto")) {
            context.write(out, businessId);
        }
    }

}
