package com.szhua.utils;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class WordCountDataUtils {

    public static final List<String> WORD_LIST = Arrays.asList("Spark", "Hadoop", "HBase", "Storm", "Flink", "Hive");



    public static final List<String> WORD_LIST1 = Arrays.asList("Spark", "Spark", "Spark","Spark","Spark","Spark");


    private static  String generateSlopeData(){
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 1000000; i++) {
            Random random = new Random();
            int endIndex = random.nextInt(WORD_LIST1.size()) % (WORD_LIST1.size()) + 1;
            String line = StringUtils.join(WORD_LIST1.toArray(), "\t", 0, endIndex);
            if (i%1000==1){
                line+="\t"+WORD_LIST.get(random.nextInt(WORD_LIST.size()-1));
            }
            builder.append(line).append("\n");
        }
        return  builder.toString();
    }

    /**
     * 模拟产生词频数据
     *
     * @return 词频数据
     */
    private static String generateData() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            Collections.shuffle(WORD_LIST);
            Random random = new Random();
            int endIndex = random.nextInt(WORD_LIST.size()) % (WORD_LIST.size()) + 1;
            String line = StringUtils.join(WORD_LIST.toArray(), "\t", 0, endIndex);
            builder.append(line).append("\n");
        }
        return builder.toString();
    }


    /**
     * 模拟产生词频数据并输出到本地
     *
     * @param outputPath 输出文件路径
     */
    private static void generateDataToLocal(String outputPath) {
        try {
            java.nio.file.Path path = Paths.get(outputPath);
            if (Files.exists(path)) {
                Files.delete(path);
            }
            Files.write(path, generateSlopeData().getBytes(), StandardOpenOption.CREATE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 模拟产生词频数据并输出到HDFS
     *
     * @param hdfsUrl          HDFS地址
     * @param user             hadoop用户名
     * @param outputPathString 存储到HDFS上的路径
     */
    private static void generateDataToHDFS(String hdfsUrl, String user, String outputPathString) {
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(new URI(hdfsUrl), new Configuration(), user);
            Path outputPath = new Path(outputPathString);
            if (fileSystem.exists(outputPath)) {
                fileSystem.delete(outputPath, true);
            }
            FSDataOutputStream out = fileSystem.create(outputPath);
            out.write(generateData().getBytes());
            out.flush();
            out.close();
            fileSystem.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        generateDataToLocal("/Users/szhua/IdeaProjects/BigDataWork/data/work/slope.txt");
        //generateDataToHDFS("hdfs://home:8020", "root", "/wordcount/input.txt");
    }
}
