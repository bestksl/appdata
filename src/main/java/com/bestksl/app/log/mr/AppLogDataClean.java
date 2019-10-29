package com.bestksl.app.log.mr;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author HaoxuanLi  Github:bestksl
 * @version created date：2019-10-29 15:01
 */
public class AppLogDataClean {
    public static class AppLogDataCleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        // 减轻mapper负担
        Text k = new Text();
        NullWritable v = NullWritable.get();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject jo = JSON.parseObject(value.toString());
            JSONObject headerObj = jo.getJSONObject(GlobalConstants.HEADER);

            // 过滤缺失必选字段的数据
            if (StringUtils.isNoneBlank("sdk_ver")) {
                return;
            }
            if (headerObj.getString("time_zone") == null || "".equals(headerObj.getString("time_zone").trim())) {
                return;
            }
            if (headerObj.getString("commit_time") == null || "".equals(headerObj.getString("commit_time").trim())) {
                return;
            }
            if (headerObj.getString("commit_id") == null || "".equals(headerObj.getString("commit_id").trim())) {
                return;
            }
            if (headerObj.getString("pid") == null || "".equals(headerObj.getString("pid").trim())) {
                return;
            }
            if (headerObj.getString("app_token") == null || "".equals(headerObj.getString("app_token").trim())) {
                return;
            }
            if (headerObj.getString("app_id") == null || "".equals(headerObj.getString("app_id").trim())) {
                return;
            }
            if (headerObj.getString("device_id") == null || "".equals(headerObj.getString("device_id").trim())) {
                return;
            }
            if (headerObj.getString("device_id_type") == null || "".equals(headerObj.getString("device_id_type").trim())) {
                return;
            }
            if (headerObj.getString("app_ver_name") == null || "".equals(headerObj.getString("app_ver_name").trim())) {
                return;
            }
            if (headerObj.getString("os_name") == null || "".equals(headerObj.getString("os_name").trim())) {
                return;
            }
            if (headerObj.getString("release_channel") == null || "".equals(headerObj.getString("release_channel").trim())) {
                return;
            }
            if (headerObj.getString("os_ver") == null || "".equals(headerObj.getString("os_ver").trim())) {
                return;
            }
            if (headerObj.getString("language") == null || "".equals(headerObj.getString("language").trim())) {
                return;
            }
            if (headerObj.getString("country") == null || "".equals(headerObj.getString("country").trim())) {
                return;
            }
            if (headerObj.getString("manufacture") == null || "".equals(headerObj.getString("manufacture").trim())) {
                return;
            }
            if (headerObj.getString("device_model") == null || "".equals(headerObj.getString("device_model").trim())) {
                return;
            }
            if (headerObj.getString("resolution") == null || "".equals(headerObj.getString("resolution").trim())) {
                return;
            }
            if (headerObj.getString("net_type") == null || "".equals(headerObj.getString("net_type").trim())) {
                return;
            }

            /*
            生成UserId
             */
            String user_id;
            if ("android".equals(headerObj.getString("os_name"))) {
                user_id = StringUtils.isNoneBlank(headerObj.getString("android_id")) ? headerObj.getString("android_id") : headerObj.getString("device_id");
            } else {
                user_id = headerObj.getString("device_id");
            }

            // 拼入生成的id
            headerObj.put("user_id", user_id);
            k.set(JsonToStringUtil.toString(headerObj));


            context.write(k, v);
        }

        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf);

            job.setJarByClass(AppLogDataClean.class);

            job.setMapperClass(AppLogDataCleanMapper.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            job.setNumReduceTasks(2);

            FileInputFormat.setInputPaths(job, new Path("/Users/haoxuanli/Desktop/input"));
            FileOutputFormat.setOutputPath(job, new Path("/Users/haoxuanli/Desktop/output"));

            boolean res = job.waitForCompletion(true);
            
            System.exit(res ? 0 : 1);
        }
    }

}
