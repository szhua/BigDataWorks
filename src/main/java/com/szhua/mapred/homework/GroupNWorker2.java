package com.szhua.mapred.homework;
import com.szhua.utils.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @Author SZhua
 * @Date 2021/3/1
 * @Description: 求 GroupTopN数据
 * 27,Mate Xs,18666
 */
public class GroupNWorker2 {


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
        job.setJarByClass(GroupNWorker2.class);

        //设置分组；
        job.setGroupingComparatorClass(InfoGroupComparator.class);


        // 3、设置Mapper和Reducer
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);


        // 4、设置Mapper阶段输出数据的key和value的类型
        job.setMapOutputKeyClass(Info.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5、设置Reducer阶段输出数据的key和value的类型
        job.setOutputKeyClass(Info.class);
        job.setOutputValueClass(NullWritable.class);

        // 6、设置输入和输出数据的路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7、提交Job
        final boolean result = job.waitForCompletion(true);  // *****
        System.exit(result ? 0 : 1);

    }


    static  class  InfoGroupComparator extends WritableComparator{
        public InfoGroupComparator() {
            super(Info.class,true);
        }
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
             Info info1 = (Info) a;
             Info info2 = (Info) b;
             return  info1.id-info2.id;
        }
    }

    static  class  Info implements WritableComparable<Info>{

        public Info() {
        }

        public Info(Integer id, String like, Double price) {
            this.id = id;
            this.like = like;
            this.price = price;
        }

        Integer id ;
        String like ;
        Double price ;


        @Override
        public int hashCode() {
            return Objects.hash(id, like, price);
        }

        @Override
        public void write(DataOutput out) throws IOException {
           out.writeInt(id);
           out.writeUTF(like);
           out.writeDouble(price);
        }
        @Override
        public void readFields(DataInput in) throws IOException {
             this.id =in.readInt() ;
             this.like =in.readUTF();
             this.price=in.readDouble();
        }

        @Override
        public int compareTo(Info o) {
            if (this.id - o.id==0){
                return  (this.price-o.price)>0?-1:1;
            }
            return this.id-o.id;
        }

        @Override
        public String toString() {
            return  id+"\t"+ like+"\t"+ price;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getLike() {
            return like;
        }

        public void setLike(String like) {
            this.like = like;
        }

        public Double getPrice() {
            return price;
        }

        public void setPrice(Double price) {
            this.price = price;
        }
    }




   static  class  MyMapper extends Mapper<LongWritable, Text, Info, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split(",");
            Integer  studentId  = Integer.parseInt(splits[0]) ;
            Double  price = Double.parseDouble(splits[2]);
            String like = splits[1];
            context.write(new Info(studentId,like,price),new IntWritable(1));
        }
    }
   static class  MyReducer extends Reducer<Info,IntWritable,Info,NullWritable>{

       int topN = 1;

       @Override
       protected void setup(Context context) throws IOException, InterruptedException {
           this.topN = context.getConfiguration().getInt(TOP_NUMBER_KEY,1);
       }

       @Override
       protected void reduce(Info key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
           int index= 0 ;
           while (values.iterator().hasNext()){
               values.iterator().next();
               if (index<=topN) {
                   System.err.println(key);
                   System.err.println(key.hashCode());
                   context.write(key,NullWritable.get());
               }
           }
           System.err.println("======");
       }
   }



}
