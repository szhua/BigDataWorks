package com.szhua.mapreducetasks.readdb;

import com.szhua.WordCountApp;
import com.szhua.component.WordCountMapper;
import com.szhua.component.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MySqlDriver {



    public static void main(String[] args) throws Exception {

        String out = "/Users/szhua/IdeaProjects/BigDataWork/data/out";

        Configuration configuration = new Configuration();
        DBConfiguration.configureDB(configuration,"com.mysql.jdbc.Driver","jdbc:mysql://home:3306/bigdata","root","389894467");

        // 创建一个Job
        Job job = Job.getInstance(configuration);
        // 设置运行的主类
        job.setJarByClass(MySqlDriver.class);

        // 设置Mapper和Reducer
        job.setMapperClass(MyMapper.class);

        // 设置Mapper输出key和value的类型
        job.setMapOutputKeyClass(MySqlWritable.class);
        job.setMapOutputValueClass(NullWritable.class);




        FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path(out);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        String [] columns =new String[]{
                "deptno",
                "dname",
                "loc"
        };
        DBInputFormat.setInput(job, MySqlWritable.class,"dept","","",columns);




        //  job.getConfiguration().set(FileOutputFormat.OUTDIR,outputPath.toString());
        FileOutputFormat.setOutputPath(job, outputPath);


        job.setNumReduceTasks(0);
        //将作业提交到群集并等待它完成，参数设置为true代表打印显示对应的进度
        boolean result = job.waitForCompletion(true);

        //关闭之前创建的fileSystem
        fileSystem.close();

        //根据作业结果,终止当前运行的Java虚拟机,退出程序
        System.exit(result ? 0 : -1);
    }


    static  class  MyMapper extends Mapper<LongWritable,MySqlWritable,MySqlWritable, NullWritable>{
        @Override
        protected void map(LongWritable key, MySqlWritable value, Context context) throws IOException, InterruptedException {
            context.write(value,NullWritable.get());
        }
    }
}
