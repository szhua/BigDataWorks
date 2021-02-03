package com.szhua.component;

import com.szhua.utils.WordCountDataUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner<Text, IntWritable> {


    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        return WordCountDataUtils.WORD_LIST.indexOf(text.toString());
    }
}
