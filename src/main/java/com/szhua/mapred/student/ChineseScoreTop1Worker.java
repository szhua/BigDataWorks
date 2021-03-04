package com.szhua.mapred.student;

import com.szhua.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * @Author SZhua
 * @Date 2021/3/1
 * @Description: 语文科目平均成绩
 */

public class ChineseScoreTop1Worker {

    public static void main(String[] args) throws Exception {

        String input = "/Users/szhua/IdeaProjects/BigDataWork/data/work/student.txt";
        String output = "/Users/szhua/IdeaProjects/BigDataWork/data/out";


        final Configuration configuration = new Configuration();

        // 1、获取Job对象
        final Job job = Job.getInstance(configuration);

        // 删除输出文件夹
        FileUtils.deleteTarget(configuration,output);

        // 2、设置class
        job.setJarByClass(ChineseScoreTop1Worker.class);

        // 3、设置Mapper和Reducer
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 4、设置Mapper阶段输出数据的key和value的类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 5、设置Reducer阶段输出数据的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6、设置输入和输出数据的路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7、提交Job
        final boolean result = job.waitForCompletion(true);  // *****
        System.exit(result ? 0 : 1);

    }

    static  class  MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split(" ");
            String subject = splits[4];
            int score = Integer.parseInt(splits[5]);
            if (subject.equals("chinese")) {
                context.write(new IntWritable(score), new Text(subject));
            }
        }
    }
    static class  MyReducer extends Reducer<IntWritable,Text, Text,IntWritable> {
        int maxScore = 0 ;
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            maxScore = key.get();
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("chinese"),new IntWritable(maxScore));
        }
    }
}
