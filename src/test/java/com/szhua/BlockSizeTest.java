package com.szhua;

import com.szhua.hdfs.ListSuffix;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

public class BlockSizeTest {

    private static final String HDFS_PATH = "hdfs://hadoop001:8020";
    private static final String HDFS_USER = "hadoop";
    private static FileSystem fileSystem;
    private ListSuffix listSuffix ;


    @Before
    public void setUp() throws  Exception{
        Configuration configuration = new Configuration();
        // 这里我启动的是单节点的 Hadoop,所以副本系数设置为 1,默认值为 3
        configuration.set("dfs.replication", "1");
        configuration.set("dfs.client.use.datanode.hostname","true");
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, HDFS_USER);
        listSuffix = new ListSuffix();
    }
    
    @After
    public  void cleanUp() throws  Exception{
        if (fileSystem!=null){
            fileSystem.close();
        }
        
    }


    @Test
    public void  suffixTest() throws  Exception{
       int count =  listSuffix.list(fileSystem,"/hdfsapi/","java");
       System.err.println(count);
    }


    @Test
    public void readAccess() throws Exception{
        FSDataInputStream input = fileSystem.open(new Path("/hdfsapi/access.log"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(input,"utf-8"));
        String str = "";
        while ((str=reader.readLine())!=null){
            parseLine(str);
        }
        input.close();
        reader.close();
    }

    private void parseLine(String line ){
      String ip =  line.split(" ")[0] ;
      String [] upDowns =line.split("\"")[2].trim().split(" ");
      long  up =  Long.parseLong(upDowns[0]);
      long  down =  Long.parseLong(upDowns[1]);
      System.err.println("ip:"+ip+"   up:"+up+"   down:"+down);
    }

    
    @Test
    public void  blockSizeTest() throws  Exception{

        FileStatus[] fileStatus = fileSystem.listStatus(new Path("/hdfsapi/"));

        for (FileStatus status : fileStatus) {
            if (status.isFile()){
                long  defaultBlockSize =  fileSystem.getDefaultBlockSize(status.getPath());
                BlockLocation[] fileBocks = fileSystem.getFileBlockLocations(
                        status.getPath(),
                        0,
                        status.getLen()
                );
                int notEnough = 0 ;
                for (int i = 0; i < fileBocks.length; i++) {
                    BlockLocation fileBlock = fileBocks[i];
                    System.err.println("block"+i+"==>"+fileBlock.getLength()/1024/1024);
                    if(fileBlock.getLength()!=defaultBlockSize){
                        notEnough++;
                    }
                }
                System.err.println(notEnough+"/"+fileBocks.length+"M");

            }else{
                System.err.println("----");
            }
        }
    }
    




}
