package com.HW1.Problem4;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Problem4Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private HashMap<String, String> businessStanford = new HashMap<String, String>();

    private String stanfordIds = "STANFORD";

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        String myfilepath = conf.get("myfilepath");
        Path part = new Path("hdfs://cshadoop1" + myfilepath);

        FileSystem fs = FileSystem.get(conf);

        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(part)));
        String line;
        line = br.readLine();
        while (line != null) {
            String[] values = line.split("\\^");
            if (values[1].contains("Stanford")) {
                businessStanford.put(values[0], stanfordIds);
            }
            line = br.readLine();
        }
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] line = value.toString().split("\\^");

        String businessId = line[2];
        String userId = line[1];
        String ratings = line[3];

        String stanfordBusinessId = businessStanford.get(businessId);

        if (stanfordBusinessId != null) {
            context.write(new Text(userId), new Text(ratings));
        }

    }

}
