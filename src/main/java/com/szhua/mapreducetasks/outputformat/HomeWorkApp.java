package com.szhua.mapreducetasks.outputformat;

import com.szhua.utils.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import static com.szhua.mapreducetasks.outputformat.MyCounter.*;

/**
 * @Author SZhua
 * @Date 2021/3/6
 * @Description: say st
 *
 *
 * 1   yy.com  name1   1
 * 2   gifshow.com  name2   2
 * 3   ruozedata.com/1.html  name3   3
 * 4   ruozedata.com/2.html  name4   4
 * 5   ruozedata.com/3.html  name5   5
 * 6   ruozedata.com/4.html  name6   6
 * 7   dongqiudi.com  name7   7
 * 8   ruozedata.com  name8   aaaaa
 * 9   ruozedata.com  7
 *
 *     1) ruozedata域名的只要id url name字段，分隔符是\t
 *     2) ruozedata域名的输出到ruozedata文件夹下
 *     3) 其他域名的只要id url age字段，分隔符是空格
 *     4) 其他域名的输出到other文件夹下
 *     5) 将这个批次处理的数据的总条数，脏数据条数输出到MySQL的一个表中
 *
 */

public class HomeWorkApp {


    public static void main(String[] args)  throws  Exception{



        String input = "/Users/szhua/IdeaProjects/BigDataWork/data/work/ouput.txt";
        String output = "/Users/szhua/IdeaProjects/BigDataWork/data/ouput/";


        final Configuration configuration = new Configuration();


        DBConfiguration.configureDB(configuration,"com.mysql.jdbc.Driver","jdbc:mysql://szhua:3306/bigdata","root","Lei389894467");


        final Job job = Job.getInstance(configuration);
        FileUtils.deleteTarget(configuration,output);
        job.setJarByClass(HomeWorkApp.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        TextInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setNumReduceTasks(2);

        job.setOutputFormatClass(MyOutputFormat.class);
        final boolean result = job.waitForCompletion(true);

        if (result){
            handleMySQL(job);
        }


    }


private static  void handleMySQL(Job job) throws  Exception{
    long  allCount = 0 ;
    long  errorCount =0 ;
    for (CounterGroup counter : job.getCounters()) {
        if (counter.getName().equals(MyCounter.class.getName())){
            for (Counter valuesCounter : counter) {
                if (valuesCounter.getName().equals(ALL_COUNTER.name())){
                    allCount = valuesCounter.getValue();
                }else if (valuesCounter.getName().equals(DIRTY_COUNTER.name())){
                    errorCount =valuesCounter.getValue();
                }
            }
        }
    }
    DBConfiguration dbConf = new DBConfiguration(job.getConfiguration());
    Connection connection = dbConf.getConnection();
    createTable(connection);
    insertData(connection,(int)allCount,(int)errorCount);
}


    private static void  insertData(Connection connection ,int allCount ,int errorCount){
        try {
            String sql ="insert into  insertInfo (allCount,dirtyCount) values ( "+allCount+" ,"+ errorCount +  "   )";
            Statement stateMent = connection.createStatement();
            stateMent.executeUpdate(sql);
            stateMent.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    private static  void  createTable(Connection connection) {
        try {
            String sql ="create table if not exists insertInfo ( " +
                    "  id bigint(0) NOT NULL AUTO_INCREMENT ," +
                    "  allCount int(255) NULL ," +
                    "  dirtyCount int(255) NUll , " +
                    "  createDate datetime NOT NULL DEFAULT CURRENT_TIMESTAMP  ON UPDATE CURRENT_TIMESTAMP ," +
                    "  PRIMARY KEY (`id`) "+
                    " )";
            Statement stateMent = connection.createStatement();
            stateMent.executeLargeUpdate(sql);
            stateMent.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter(MyCounter.ALL_COUNTER).increment(1);
            String[] splits = value.toString().split("\t");
            if (splits.length<4){
                context.getCounter(DIRTY_COUNTER).increment(1);
                return;
            }
            try{
                Integer.parseInt(splits[3].trim());
            }catch (Exception e){
                context.getCounter(DIRTY_COUNTER).increment(1);
                return;
            }
            if (splits[1].contains("ruozedata.com")){
                context.write(new Text(splits[1]),
                        new Text(StringUtils.joinWith("\t",splits[0],splits[1],splits[2])+"\r\n"));
            }else{
                context.write(new Text(splits[1]),
                        new Text(StringUtils.joinWith(" ",splits[0],splits[1],splits[3])+"\r\n"));
            }
        }
    }



    public  static  class  MyReducer extends Reducer<Text,Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
               for (Text value : values) {
                   context.write(value, NullWritable.get());
               }
        }

    }

    public  static  class  MyOutputFormat  extends  FileOutputFormat<Text,NullWritable>{

        @Override
        public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
            return new MyRecordWriter(job);
        }
    }

    public static class MyRecordWriter extends RecordWriter<Text, NullWritable> {

        FileSystem fileSystem;
        FSDataOutputStream ruozedataOut;
        FSDataOutputStream otherOut;

        public MyRecordWriter(TaskAttemptContext job) {

            TaskID taskId = job.getTaskAttemptID().getTaskID();
            int partition = taskId.getId();

            try {
                fileSystem = FileSystem.get(job.getConfiguration());
                Path ruozePath = new Path(job.getConfiguration().get(FileOutputFormat.OUTDIR)+"/ruozedata/log"+partition+".text");
                ruozedataOut =fileSystem.create(ruozePath);
                Path otherPath = new Path(job.getConfiguration().get(FileOutputFormat.OUTDIR)+"/other/log"+partition+".text");
                otherOut = fileSystem.create(otherPath);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void write(Text key, NullWritable value) throws IOException, InterruptedException {
            if(key.toString().contains("ruozedata")) {
                ruozedataOut.write(key.toString().getBytes());
            } else {
                otherOut.write(key.toString().getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            IOUtils.closeStream(ruozedataOut);
            IOUtils.closeStream(otherOut);
            fileSystem.close();
        }
    }
}
