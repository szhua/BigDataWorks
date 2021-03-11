package com.szhua.har;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

//开始之前应该对小文件进行打包处理
// 对输出的文件进行全部打包；
// hadoop archive -archiveName foo.har  -p  /data/out    /har/out
// hadoop fs -ls  har:////har/out/foo.har
// hadoop fs -ls  /har/out/foo.har

public class HarAppHarCmd {


    private static final String HDFS_PATH = "hdfs://hadoop000:8020";
    private static final String HDFS_USER = "szhua";

    public static void main(String[] args) throws Exception{
        //String basePath = System.getProperty("user.dir");
        if(args==null||args.length==0) {
             args = new String[]{
                     //"/data/out/foo.har",
                     "har:////har/out/foo.har",
                     "/data/har2/"
             };
         }
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
        configuration.set("dfs.replication", "1");
        configuration.set("dfs.client.use.datanode.hostname","true");

        // 创建一个Job
        Job job = Job.getInstance(configuration);

        // 设置运行的主类
        job.setJarByClass(HarAppHarCmd.class);

        // 设置Mapper和Reducer
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // 设置Mapper输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置Reducer输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //如果输出目录已经存在，则必须先删除，否则重复运行程序时会抛出异常
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, HDFS_USER);
        //FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path(args[1]);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        //设置作业输入文件和输出文件的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
      //  job.getConfiguration().set(FileOutputFormat.OUTDIR,outputPath.toString());
        FileOutputFormat.setOutputPath(job, outputPath);
        //将作业提交到群集并等待它完成，参数设置为true代表打印显示对应的进度
        boolean result = job.waitForCompletion(true);


        //关闭之前创建的fileSystem
        fileSystem.close();

        //根据作业结果,终止当前运行的Java虚拟机,退出程序
        System.exit(result ? 0 : -1);
    }



    static  class  MyMapper extends Mapper<LongWritable,Text,Text, NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.err.println(value);
            context.write(value,NullWritable.get());
        }
    }

    static class  MyReducer extends Reducer<Text,NullWritable,Text,NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }

}
