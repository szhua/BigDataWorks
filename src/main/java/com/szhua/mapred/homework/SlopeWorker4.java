package com.szhua.mapred.homework;

import com.szhua.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.Random;

/**
 * @Author SZhua
 * @Date 2021/3/1
 * @Description: job 合并；
 */

public class SlopeWorker4 {


  private  static String input = "/Users/szhua/IdeaProjects/BigDataWork/data/work/slope.txt";
  private  static String outputScatter = "/Users/szhua/IdeaProjects/BigDataWork/data/scatter";
  private  static  String output = "/Users/szhua/IdeaProjects/BigDataWork/data/out";


    public static void main(String[] args) throws Exception {

        long current = System.currentTimeMillis();
        Job job1 = createScatterJob() ;
        Job job2 =createCombineJob();
        JobConf jobConf =new JobConf(SlopeWorker4.class);
        ControlledJob controlledJob1 =new ControlledJob(jobConf);
        controlledJob1.setJob(job1);
        ControlledJob controlledJob2 =new ControlledJob(jobConf);
        controlledJob2.setJob(job2);
        controlledJob2.addDependingJob(controlledJob1);
        JobControl jobControl =new JobControl("controllerS");
        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);
        Thread thread =new Thread(jobControl);
        thread.start();

        while (true){
            if (jobControl.allFinished()){
                System.err.println("OK");
                System.err.println(((System.currentTimeMillis()-current)/1000)+"S");
                jobControl.stop();
                break;
            }
        }




    }

    private static Job createScatterJob() throws Exception{
        final Configuration configuration = new Configuration();
        configuration.setInt(SLOPE_NUMBER,10);
        Job job = Job.getInstance(configuration);
        FileUtils.deleteTarget(configuration,outputScatter);
        job.setJarByClass(SlopeWorker4.class);
        job.setMapperClass(MyMapper1.class);
        job.setReducerClass(MyReducer1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setNumReduceTasks(10/2);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outputScatter));
        return  job ;


    }
    private static  Job createCombineJob() throws Exception{
        final Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        FileUtils.deleteTarget(configuration,output);
        job.setJarByClass(SlopeWorker4.class);
        job.setMapperClass(MyMapper2.class);
        job.setReducerClass(MyReducer2.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path(outputScatter));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return  job ;
    }


    private  static  final  String  SLOPE_NUMBER = "SLOPE_NUMBER";
    static class  MyMapper1 extends Mapper<LongWritable, Text,Text, IntWritable> {

        private int slopNumber =1 ;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            slopNumber =context.getConfiguration().getInt(SLOPE_NUMBER,1);
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            for (String split : splits) {
                context.write(new Text(split+"-"+new Random().nextInt(slopNumber)),new IntWritable(1));
            }
        }
    }

    static   class  MyReducer1 extends Reducer<Text,IntWritable,Text,IntWritable> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0 ;
            for (IntWritable ignore : values) {
                count++ ;
            }
            context.write(key,new IntWritable(count));
        }
    }


    static class  MyMapper2 extends Mapper<LongWritable, Text,Text, IntWritable>{


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            String word  =  splits[0].split("-")[0];
            context.write(new Text(word),new IntWritable(Integer.parseInt(splits[1])));
        }
    }


    static   class  MyReducer2 extends Reducer<Text,IntWritable,Text,IntWritable>{

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0 ;
            for (IntWritable value : values) {
                count+=value.get();
            }
            context.write(key,new IntWritable(count));
        }
    }


}


