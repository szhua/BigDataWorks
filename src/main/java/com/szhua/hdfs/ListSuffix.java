package com.szhua.hdfs;


import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ListSuffix {

    public int   list(FileSystem fileSystem , String dic , String suffix) throws Exception{
        FileStatus[] fileStatus = fileSystem.listStatus(new Path(dic));
        int count = 0 ;
        for (FileStatus status : fileStatus) {
             if (status.isFile()){
               String name =  status.getPath().getName();
               if (name.contains("."+suffix)){
                   System.err.println(name);
                   count++;
                   fileSystem.delete(status.getPath(),true);
               }
             }
        }
        System.err.println(suffix+"文件的个数:=="+count);
        return  count ;
    }




}
