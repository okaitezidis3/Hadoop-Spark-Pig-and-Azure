package edu.gatech.cse6242;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Q4 {

  public static class Mapper1
        extends Mapper<Object, Text, Text, IntWritable>{
            //Designate in_nodes with -1 and out_nodes with 1
            private Text src = new Text();
            private IntWritable map_out_nodes = new IntWritable(1);
            private Text target = new Text();
            private IntWritable map_in_nodes = new IntWritable(-1);


        public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
        while (itr.hasMoreTokens()) {
            src.set(itr.nextToken());
            target.set(itr.nextToken());
            context.write(src, map_out_nodes);
            context.write(target, map_in_nodes);
        }
      }
    }



  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
          sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
  }
}

  public static class Mapper2
        extends Mapper<Object, Text, Text, IntWritable>{
            //Designate Numbcount as 1
            private Text diff = new Text();
            private IntWritable numb_count = new IntWritable(1);
            private Text temp = new Text();


        public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
        while (itr.hasMoreTokens()) {
            temp.set(itr.nextToken());
            diff.set(itr.nextToken());
            context.write(diff, numb_count);
        }
      }
    }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job1 = Job.getInstance(conf, "J1");
    //Job 1
    job1.setJarByClass(Q4.class);
    job1.setMapperClass(Mapper1.class);
    job1.setCombinerClass(IntSumReducer.class);
    job1.setReducerClass(IntSumReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    //Intermediate File for Job 2
    FileOutputFormat.setOutputPath(job1, new Path(args[1]+"inter"));
    job1.waitForCompletion(true);

    //Job 2
    Job job2 = Job.getInstance(conf, "J2");
    job2.setJarByClass(Q4.class);
    job2.setMapperClass(Mapper2.class);
    job2.setCombinerClass(IntSumReducer.class);
    job2.setReducerClass(IntSumReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path(args[1]+"inter"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
//Reference https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
//Hadoop: The Definitive Guide
