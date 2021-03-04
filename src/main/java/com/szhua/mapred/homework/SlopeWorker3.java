package com.szhua.mapred.homework;

import com.szhua.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Random;

/**
 * @Author SZhua
 * @Date 2021/3/1
 * @Description: 数据倾斜未进行处理的情况
 */

public class SlopeWorker3 {


    /**
     * Flink	202
     * HBase	179
     * Hadoop	188
     * Spark	3498997
     * Storm	222
     *
     *
     * Flink	202
     * HBase	179
     * Hadoop	188
     * Spark	3498997
     * Storm	222
     *
     */

    public static void main(String[] args)  throws  Exception{


        long current = System.currentTimeMillis() ;


        String input = "/Users/szhua/IdeaProjects/BigDataWork/data/work/slope.txt";
        String output = "/Users/szhua/IdeaProjects/BigDataWork/data/out";


        final Configuration configuration = new Configuration();

        // 1、获取Job对象
        final Job job = Job.getInstance(configuration);

        // 删除输出文件夹
        FileUtils.deleteTarget(configuration,output);

        // 2、设置class
        job.setJarByClass(SlopeWorker3.class);

        // 3、设置Mapper和Reducer
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 4、设置Mapper阶段输出数据的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5、设置Reducer阶段输出数据的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(5);

        // 6、设置输入和输出数据的路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7、提交Job
        final boolean result = job.waitForCompletion(true);  // *****

        System.err.println(((System.currentTimeMillis()-current)/1000)+"S");


        System.exit(result ? 0 : 1);

    }

   static class  MyMapper extends Mapper<LongWritable, Text,Text, IntWritable>{


       @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            for (String split : splits) {
                context.write(new Text(split),new IntWritable(1));
            }
        }
    }


   static   class  MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{


       @Override
       protected void setup(Context context) throws IOException, InterruptedException {
           super.setup(context);
       }

       @Override
       protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
           int count = 0 ;
           for (IntWritable value : values) {
                count++ ;
           }
           context.write(key,new IntWritable(count));
       }
   }





}
