package com.bestksl.app.log.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @author HaoxuanLi  Github:bestksl
 * @version created date：2019-10-29 15:01
 */
public class AppLogDataClean {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        System.setProperty("HADOOP_USER_NAME", "root");


        Job job = Job.getInstance(conf);
        job.setJarByClass(AppLogDataClean.class);

        job.setMapperClass(AppLogDataCleanMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);
        FileInputFormat.setInputPaths(job, new Path(args[0]));  //"/Users/haoxuanli/Desktop/input/20170101"
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  //"/Users/haoxuanli/Desktop/output"

        // 懒加载output模式 防止因为多路输出时 没有文件但是依然创建旧目录
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        System.out.println(args[0]);
        System.out.println(args[1]);
        // Delete output if exists
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(args[1])))
            hdfs.delete(new Path(args[1]), true);

        // Execute job
        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);
    }

}
