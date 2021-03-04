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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * @Author SZhua
 * @Date 2021/3/1
 * @Description: 求 TopN数据
 * 矿泉水,1
 */
public class TopNWorker {


    public static void main(String[] args) throws Exception {


        String input = "/Users/szhua/IdeaProjects/BigDataWork/data/work/topn.txt";
        String output = "/Users/szhua/IdeaProjects/BigDataWork/data/out";


        final Configuration configuration = new Configuration();
        configuration.set("topNumber","3");

        // 1、获取Job对象
        final Job job = Job.getInstance(configuration);

        // 删除输出文件夹
        FileUtils.deleteTarget(configuration,output);

        // 2、设置class
        job.setJarByClass(TopNWorker.class);

        // 3、设置Mapper和Reducer
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 4、设置Mapper阶段输出数据的key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5、设置Reducer阶段输出数据的key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(2);

        // 6、设置输入和输出数据的路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7、提交Job
        final boolean result = job.waitForCompletion(true);  // *****
        System.exit(result ? 0 : 1);


    }

   static class  MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split(",");
            String type = splits[0] ;
            int  number = Integer.parseInt(splits[1]);
            System.err.println(number);
            context.write(new Text(type),new IntWritable(number));
        }
    }
   static class  MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{


        List<Info>  infos = new ArrayList<>();



        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                 this.infos.add(new Info(key.toString(),value.get()));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
                this.infos.sort(Comparator.comparingInt(o -> -o.value));
                int top =  Integer.parseInt(context.getConfiguration().get("topNumber","1"));
                for (int i = 0; i < this.infos.size(); i++) {
                   context.write(new Text(this.infos.get(i).key),new IntWritable(this.infos.get(i).value));
                   if (i+1==top){
                       break;
                   }
                }
        }

        class Info {

            public Info(String key, int value) {
                this.key = key;
                this.value = value;
            }

            String key;
            int value ;
        }
    }



}
