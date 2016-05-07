package com.HW1.Problem4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Problem4Driver {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();

        conf.set("myfilepath", "/yelpdatafall/business/business.csv");

        Job job = Job.getInstance(conf);
        job.setJobName("Map-Side-Join");
        job.setJarByClass(Problem4Driver.class);

        job.setMapperClass(Problem4Mapper.class);
        job.setNumReduceTasks(0);

        TextInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.waitForCompletion(true);

    }

}
