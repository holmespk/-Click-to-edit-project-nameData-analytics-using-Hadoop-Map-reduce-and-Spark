package com.HW1.Problem3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Problem3TopTenMapper extends Mapper<Object, Text, Text, Text> {

    private Text outkey = new Text();
    private Text outvalue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split("\\^");

        outkey.set(line[0]);
        outvalue.set("1" + line[1]);
        context.write(outkey, outvalue);

    }

}
