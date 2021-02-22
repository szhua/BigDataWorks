package com.szhua.accesswork;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AccessMapper extends Mapper<LongWritable, Text,Text,Access> {

    @Override
    protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        String ip =  line.toString().split(" ")[0] ;
        String [] upDowns =line.toString().split("\"")[2].trim().split(" ");
        long  up =  Long.parseLong(upDowns[0]);
        long  down =  Long.parseLong(upDowns[1]);
        Access access =new Access(ip,up,down);
        context.write(new Text(ip),access);
    }


}
