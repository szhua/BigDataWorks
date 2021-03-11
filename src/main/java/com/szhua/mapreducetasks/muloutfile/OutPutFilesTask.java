package com.szhua.mapreducetasks.muloutfile;


import com.szhua.mapred.homework.GroupNWorker;
import com.szhua.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @Author SZhua
 * @Date 2021/3/4
 * @Description: say st
 */

public class OutPutFilesTask  {
    public static void main(String[] args)  throws Exception{

        String input = "/Users/szhua/IdeaProjects/BigDataWork/data/work/grouptopn.txt";
        String output = "/Users/szhua/IdeaProjects/BigDataWork/data/out";


        final Configuration configuration = new Configuration();

        // 1、获取Job对象
        final Job job = Job.getInstance(configuration);

        // 删除输出文件夹
        FileUtils.deleteTarget(configuration,output);

        // 2、设置class
        job.setJarByClass(GroupNWorker.class);

        // 3、设置Mapper和Reducer
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 4、设置Mapper阶段输出数据的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 5、设置Reducer阶段输出数据的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 6、设置输入和输出数据的路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));


        MultipleOutputs.addNamedOutput(job,"l1", TextOutputFormat.class,Text.class,NullWritable.class);
        MultipleOutputs.addNamedOutput(job,"l2", TextOutputFormat.class,Text.class,NullWritable.class);
        MultipleOutputs.addNamedOutput(job,"l3", TextOutputFormat.class,Text.class,NullWritable.class);


        // 7、提交Job
        final boolean result = job.waitForCompletion(true);  // *****
        System.exit(result ? 0 : 1);

    }


   static class MyMapper extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split(",");
            context.write(new Text(splits[0]),new Text(splits[1]));

        }
    }
   static  class  MyReducer extends Reducer<Text,Text,Text, NullWritable>{

        private MultipleOutputs multipleOutputs ;

       @Override
       protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs(context);
       }

       @Override
       protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
             multipleOutputs.write("l1",key,NullWritable.get(),"l1");
             multipleOutputs.write("l2",key,NullWritable.get(),"l2");
             multipleOutputs.write("l3",key,NullWritable.get(),"l3");
       }
   }






}
