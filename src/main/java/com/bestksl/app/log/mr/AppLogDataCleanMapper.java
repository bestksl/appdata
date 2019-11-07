package com.bestksl.app.log.mr;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author HaoxuanLi  Github:bestksl
 * @version created date：2019-11-04 22:35
 */
public class AppLogDataCleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text k;
    private NullWritable v;
    private SimpleDateFormat sdf;
    private MultipleOutputs<Text, NullWritable> multipleOutputs; // 多路输出器

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 减轻mapper负担
        k = new Text();
        v = NullWritable.get();
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        multipleOutputs = new MultipleOutputs<>(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        JSONObject jo = JSON.parseObject(value.toString());
        JSONObject headerObj = jo.getJSONObject(GlobalConstants.HEADER);

        // 过滤缺失必选字段的数据
        if (!StringUtils.isNoneBlank("sdk_ver")) {
            return;
        }
        if (headerObj.getString("time_zone") == null || "".equals(headerObj.getString("time_zone").trim())) {
            return;
        }
        if (headerObj.getString("commit_time") == null || "".equals(headerObj.getString("commit_time").trim())) {
            return;
        } else {
            // 仅供测试练习
            String commit_time = headerObj.getString("commit_time");
            long l = Long.parseLong(commit_time);
            long now = System.currentTimeMillis();
            headerObj.put("commit_time", sdf.format(new Date(l + now - l - 24 * 60 * 60 * 1000)));
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
        if (headerObj.getString("device_id") == null || headerObj.getString("device_id").length() < 17) {
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
        if ("android".equals(headerObj.getString("os_name").trim())) {
            multipleOutputs.write(k, v, "android/android");
        } else {
            multipleOutputs.write(k, v, "ios_mac/ios_mac");
        }


    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // 关闭资源
        multipleOutputs.close();
    }


}
