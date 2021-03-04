package com.szhua.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author SZhua
 * @Date 2021/3/1
 * @Description: say st
 */

public class FileUtils {

    public  static  void deleteTarget(Configuration configuration ,String path ) throws Exception{
        FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path(path);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
    }

    public static void main(String[] args) {

        IntWritable a =new IntWritable(1);
        IntWritable b =new IntWritable(2);
        List<IntWritable> all = new ArrayList<>() ;
        all.add(a);
        all.add(b);
        System.err.println(all);
    }
}
