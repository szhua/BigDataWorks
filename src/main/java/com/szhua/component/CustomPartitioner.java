package com.szhua.component;

import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner {

    public int getPartition(Object o, Object o2, int i) {
        return 0;
    }
}
