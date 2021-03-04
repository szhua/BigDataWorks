package com.szhua;

import com.szhua.component.WordCountMapper;
import com.szhua.component.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class WordCountApp {


    private static final String HDFS_PATH = "hdfs://home:8020";
    private static final String HDFS_USER = "hadoop";

    public static void main(String[] args) throws Exception{
        String basePath = System.getProperty("user.dir");


        if(args==null||args.length==0) {
             args = new String[]{
                     basePath+"/data/input.txt",
                     basePath+"/data/out/"
             };
         }
        //  文件输入路径和输出路径由外部传参指定
        if (args.length < 2) {
            System.out.println("Input and output paths are necessary!");
            return;
        }
        // 需要指明hadoop用户名，否则在HDFS上创建目录时可能会抛出权限不足的异常
        System.setProperty("HADOOP_USER_NAME", HDFS_USER);

        Configuration configuration = new Configuration();
        // 指明HDFS的地址
        //configuration.set("fs.defaultFS", HDFS_PATH);
        //configuration.set("dfs.replication", "1");
        //configuration.set("dfs.client.use.datanode.hostname","true");

        // 创建一个Job
        Job job = Job.getInstance(configuration);

        // 设置运行的主类
        job.setJarByClass(WordCountApp.class);

        // 设置Mapper和Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置Mapper输出key和value的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置Reducer输出key和value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //如果输出目录已经存在，则必须先删除，否则重复运行程序时会抛出异常
       // FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, HDFS_USER);
        FileSystem fileSystem = FileSystem.get(configuration);
        Path outputPath = new Path(args[1]);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        //设置作业输入文件和输出文件的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
      //  job.getConfiguration().set(FileOutputFormat.OUTDIR,outputPath.toString());
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setNumReduceTasks(2);

        //将作业提交到群集并等待它完成，参数设置为true代表打印显示对应的进度
        boolean result = job.waitForCompletion(true);

        //关闭之前创建的fileSystem
        fileSystem.close();

        //根据作业结果,终止当前运行的Java虚拟机,退出程序
        System.exit(result ? 0 : -1);
    }

/**
 *
 MapReduce前期工作
 job.waitForCompletion
 => Job.submit
       1-ensureState
       2-setUseNewAPI
       3-connect      ugi - > new Cluster  Job的cluster对象有了赋值；
       4-JobSubmitter submitter = getJobSubmitter(cluster.getFileSystem(), cluster.getClient());
       5-ugi->submitter.submitJobInternal(Job.this, cluster)
                        1. checkSpecs(job)
                                       1.OutPutFormat.checkOutputSpecs(jtFs,jConf)
                                             (没有设置的化 throw InvalidJobConfException)
                                             (is exit     throw new FileAlreadyExistsException("Output directory " + outDir +
                                              " already exists") )   (job.getConfiguration().set(FileOutputFormat.OUTDIR,outputPath.toString());)

                        //工作暂存区域获得  /tmp/...... debug 进行截取即可
                        2.Path jobStagingArea = JobSubmissionFiles.getStagingDir(cluster, conf);
                        //获得jobId
                        3.JobID jobId = submitClient.getNewJobID();
                        //根据jobId然后创建新的Dictionary
                        4.Path submitJobDir = new Path(jobStagingArea, jobId.toString());
                        //使用命令行 -libjars, -files, -archives 这些配置 config
                        5.copyAndConfigureFiles(job, submitJobDir);
                        //创建job.xml的path
                        6.Path submitJobFile = JobSubmissionFiles.getJobConfPath(submitJobDir);
                        //Split；任务分片;
                        7.int maps = writeSplits(job, submitJobDir);
                        //通过InputFormat为job在逻辑上对输入的文件进行分段
                        // FileInputFormat(CombineFileInputFormat,NLineInputFormat)
                        8.mapsCount = InputFormat.getSplits(job)
                        9.........一些配置的书写
                        //在这里把各种配置文件写到了 submitJobFile 中
                        10. writeConf(conf, submitJobFile);
                        //使用submitClient进行提交作业
                        11.status = submitClient.submitJob(jobId, submitJobDir.toString(), job.getCredentials());
                        //删除暂存的文件配置'
                        12.finally ==> jtFs.delete(submitJobDir, true);
         6://TODO


 */

/**
 * FileInputFormat 的 getSplits ;
 * //minSize =1
 * 1.long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
 * // maxSize = Long.MAX
 * 2.long maxSize = getMaxSplitSize(job);
 * //generateSplits
 * 3. List<InputSplit> splits = new ArrayList<InputSplit>();
 * //listStatus
 * 4.List<FileStatus> files = listStatus(job);
 * //可以分割的情况下；
 * 5.if(isSplitable(job, path))
 * //32或者128
 * 6. long blockSize = file.getBlockSize();
 * //此处得以计算；
 * 7.long splitSize = computeSplitSize(blockSize, minSize, maxSize);
 * // SplitSlop为一个常量（为10%的溢出，如果最后逻辑file块大小为10%以下的情况下，那么不进行增加Split）
 * 8.while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {添加Splits}
 * //{增加剩余的file块为一个Split ，就是把剩下的那一块加入其中}
 * 9. if (bytesRemaining != 0)
 * //保存输入文件的数量
 * 10.job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
 * 11.
 *
 *
 *
 *
 *
 *
 *
 */



}
