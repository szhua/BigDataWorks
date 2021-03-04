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

/**
 * @Author SZhua
 * @Date 2021/3/1
 * @Description: 求 TopN数据
 * 矿泉水,1
 */
public class TopNWorker2 {


    private static final  String  TOP_NUMBER_KEY =  "TOP_NUMBER_KEY";

    public static void main(String[] args) throws Exception {


        String input = "/Users/szhua/IdeaProjects/BigDataWork/data/work/topn.txt";
        String output = "/Users/szhua/IdeaProjects/BigDataWork/data/out";


        final Configuration configuration = new Configuration();
        configuration.setInt(TOP_NUMBER_KEY,3);

        // 1、获取Job对象
        final Job job = Job.getInstance(configuration);

        // 删除输出文件夹
        FileUtils.deleteTarget(configuration,output);

        // 2、设置class
        job.setJarByClass(TopNWorker2.class);

        // 3、设置Mapper和Reducer
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 4、设置Mapper阶段输出数据的key和value的类型
        job.setMapOutputKeyClass(MyTopInt.class);
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


   static   class  MyTopInt extends  IntWritable{

        public MyTopInt(int value) {
            super(value);
        }
        public  MyTopInt(){

        }
        @Override
        public int compareTo(IntWritable o) {
            return -super.compareTo(o);
        }
    }


   static  class  MyMapper extends Mapper<LongWritable, Text, MyTopInt, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split(",");
            String type = splits[0] ;
            int  number = Integer.parseInt(splits[1]);
            context.write(new MyTopInt(number),new Text(type));
        }
    }
   static class  MyReducer extends Reducer<MyTopInt,Text,Text,IntWritable>{


        int topN = 1 ;
        int num = 0 ;

       @Override
       protected void setup(Context context)  {
           this.topN = context.getConfiguration().getInt(TOP_NUMBER_KEY,1);
       }

       @Override
       protected void reduce(MyTopInt key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           System.err.println(key.toString());
           for (Text value : values) {
               num++;
               if (num<=topN){
                   context.write(value,key);
               }
           }


       }
   }



}
