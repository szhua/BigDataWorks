package com.szhua;

import com.szhua.component.WordCountMapper;
import com.szhua.component.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class WordCountApp {


    private static final String HDFS_PATH = "hdfs://hadoop000:8020";
    private static final String HDFS_USER = "szhua";


    public static void main(String[] args) throws Exception{

         args =new String[]{
                 "/wordcount/input.txt",
                 "/wordcount/out/wordcout"
         };
        //  文件输入路径和输出路径由外部传参指定
        if (args.length < 2) {
            System.out.println("Input and output paths are necessary!");
            return;
        }
        // 需要指明hadoop用户名，否则在HDFS上创建目录时可能会抛出权限不足的异常
        System.setProperty("HADOOP_USER_NAME", HDFS_USER);

        Configuration configuration = new Configuration();
        // 指明HDFS的地址
        configuration.set("fs.defaultFS", HDFS_PATH);


        // 创建一个Job
        Job job = Job.getInstance(configuration);

        // 设置运行的主类
        job.setJarByClass(WordCountApp.class);

        // 设置Mapper和Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置Mapper输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置Reducer输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //如果输出目录已经存在，则必须先删除，否则重复运行程序时会抛出异常
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_USER), configuration, HDFS_PATH);
        Path outputPath = new Path(args[1]);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        //设置作业输入文件和输出文件的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);


        //将作业提交到群集并等待它完成，参数设置为true代表打印显示对应的进度
        boolean result = job.waitForCompletion(true);

        //关闭之前创建的fileSystem
        fileSystem.close();

        //根据作业结果,终止当前运行的Java虚拟机,退出程序
        System.exit(result ? 0 : -1);
    }

}
