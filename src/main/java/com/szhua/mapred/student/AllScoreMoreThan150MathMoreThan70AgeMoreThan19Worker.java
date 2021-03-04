package com.szhua.mapred.student;

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
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author SZhua
 * @Date 2021/3/1
 * @Description: 语文科目平均成绩
 */

public class AllScoreMoreThan150MathMoreThan70AgeMoreThan19Worker {

    public static void main(String[] args) throws Exception {

        String input = "/Users/szhua/IdeaProjects/BigDataWork/data/work/student.txt";
        String output = "/Users/szhua/IdeaProjects/BigDataWork/data/out";


        final Configuration configuration = new Configuration();

        // 1、获取Job对象
        final Job job = Job.getInstance(configuration);

        // 删除输出文件夹
        FileUtils.deleteTarget(configuration,output);

        // 2、设置class
        job.setJarByClass(AllScoreMoreThan150MathMoreThan70AgeMoreThan19Worker.class);

        // 3、设置Mapper和Reducer
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 4、设置Mapper阶段输出数据的key和value的类型
        job.setMapOutputKeyClass(Text.class);
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

    static  class  MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split(" ");
            int score = Integer.parseInt(splits[5]);
            String name =splits[1];
            int age = Integer.parseInt(splits[2]);
            String subject =splits[4];
            if (age>=19 ) {
                context.write(new Text(name), new Text(subject+"-"+score));
            }
        }
    }
    static class  MyReducer extends Reducer<Text,Text, Text,Text> {

        private Map<String, Integer> infos =new HashMap<>();
        private int totalCount = 0 ;
        private int totalScore = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalScore = 0 ;
            for (Text value : values) {
                String[] infos =  value.toString().split("-");
                String subject =   infos[0] ;
                int score = Integer.parseInt(infos[1]);
                totalScore+=score;
                if (subject.equals("math")&&score<70){
                    totalScore=0;
                    break;
                }
            }
            if (totalScore>150){
                totalCount++;
                this.totalScore+=totalScore;
                infos.put(key.toString(),totalScore);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
           context.write(new Text("tototalCount"),new Text(String.valueOf(totalCount)));
           double avg =  new BigDecimal(totalScore).divide(new BigDecimal(totalCount)).setScale(2,BigDecimal.ROUND_HALF_UP)
                    .doubleValue();
           context.write(new Text("avg"),new Text(String.valueOf(avg)));
           infos.forEach((name,score)->{
               try {
                   context.write(new Text(name),new Text(String.valueOf(score)));
               } catch (IOException e) {
                   e.printStackTrace();
               } catch (InterruptedException e) {
                   e.printStackTrace();
               }
           });
        }
    }
}
