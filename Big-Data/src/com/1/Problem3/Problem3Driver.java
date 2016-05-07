package com.HW1.Problem3;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Problem3Driver extends Configured implements Tool {

    private static final String OUTPUT_PATH = "intermediate_output";

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(new Path(args[3]))) {
            fs.delete(new Path(args[3]), true);
        }

        if (fs.exists(new Path(OUTPUT_PATH))) {
            fs.delete(new Path(OUTPUT_PATH), true);
        }

        if (args.length != 4) {
            System.err.println("Usage: Problem3Driver <Review> <Business> <JoinType> <OutPutPath>");
            System.exit(-1);
        }

        Job job = Job.getInstance(conf);
        job.setJobName("Job1");
        job.setJarByClass(getClass());

        job.setMapperClass(Problem3ReviewMapper.class);
        job.setReducerClass(Problem3ReviewReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(getClass());

        MultipleInputs.addInputPath(job2, new Path(OUTPUT_PATH), TextInputFormat.class, Problem3TopTenMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, Problem3BusinessMapper.class);
        job2.getConfiguration().set("join.type", args[2]);

        job2.setMapOutputKeyClass(Text.class);
        job2.setReducerClass(Problem3Reducer.class);

        job2.setOutputKeyClass(Text.class);

        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        return job2.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Problem3Driver driver = new Problem3Driver();
        int exitCode = ToolRunner.run(driver, args);
        System.exit(exitCode);
    }
}
