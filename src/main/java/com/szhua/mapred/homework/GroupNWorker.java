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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @Author SZhua
 * @Date 2021/3/1
 * @Description: 求 TopN数据
 * 矿泉水,1
 */
public class GroupNWorker {


    private static final  String  TOP_NUMBER_KEY =  "TOP_NUMBER_KEY";

    public static void main(String[] args) throws Exception {


        String input = "/Users/szhua/IdeaProjects/BigDataWork/data/work/grouptopn.txt";
        String output = "/Users/szhua/IdeaProjects/BigDataWork/data/out";


        final Configuration configuration = new Configuration();
        configuration.setInt(TOP_NUMBER_KEY,2);

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
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5、设置Reducer阶段输出数据的key和value的类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // 6、设置输入和输出数据的路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7、提交Job
        final boolean result = job.waitForCompletion(true);  // *****
        System.exit(result ? 0 : 1);


    }




   static  class  MyMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split(",");
            int  studentId  = Integer.parseInt(splits[0]) ;
            int  price = Integer.parseInt(splits[2]);

            context.write(new IntWritable(studentId),new IntWritable(price));
        }
    }
   static class  MyReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{

       int topN = 1 ;

       @Override
       protected void setup(Context context)  {
           this.topN = context.getConfiguration().getInt(TOP_NUMBER_KEY,1);
       }

       @Override
       protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int index = 0 ;
            List<IntWritable> prices = new ArrayList<>();
           for (IntWritable value : values) {
               prices.add(new IntWritable(value.get()));
           }
           prices.sort(Comparator.comparingInt(o->-o.get()));
           for (IntWritable price : prices) {
            index++ ;
               if (index<=topN){
                   context.write(key,price);
               }
           }
           System.err.println("======");
       }
   }



}
