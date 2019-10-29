package com.bestksl.app.log.mr;

import com.alibaba.fastjson.JSONObject;

/**
 * @author HaoxuanLi  Github:bestksl
 * @version created date：2019-10-29 18:32
 */
public class JsonToStringUtil {
    public static String toString(JSONObject jsonObj) {
        StringBuilder sb = new StringBuilder();
        sb.append(jsonObj.get("cid_sn")).append("\001")
                .append(jsonObj.get("os_ver")).append("\001")
                .append(jsonObj.get("mac")).append("\001")
                .append(jsonObj.get("resolution")).append("\001")
                .append(jsonObj.get("commit_time")).append("\001")
                .append(jsonObj.get("sdk_ver")).append("\001")
                .append(jsonObj.get("device_id_type")).append("\001")
                .append(jsonObj.get("device_model")).append("\001")
                .append(jsonObj.get("android_id")).append("\001")
                .append(jsonObj.get("app_ver_name")).append("\001")
                .append(jsonObj.get("app_ver_code")).append("\001")
                .append(jsonObj.get("pid")).append("\001")
                .append(jsonObj.get("net_type")).append("\001")
                .append(jsonObj.get("device_id")).append("\001")
                .append(jsonObj.getString("app_device_id")).append("\001")
                .append(jsonObj.getString("release_channel")).append("\001")    //用户下载该app时所用的app应用市场：360，安智市场，
                .append(jsonObj.getString("country")).append("\001")
                .append(jsonObj.getString("time_zone")).append("\001")
                .append(jsonObj.getString("os_name")).append("\001")         //操作系统名称
                .append(jsonObj.getString("manufacture")).append("\001")          //手机生产厂家
                .append(jsonObj.getString("commit_id")).append("\001")         //提交请求的序号
                .append(jsonObj.getString("app_token")).append("\001")        //app名称：
                .append(jsonObj.getString("app_id")).append("\001")         //app的id标识(所属的公司事业部)
                .append(jsonObj.getString("language")).append("\001")     //用户的操作系统语言（）
                .append(jsonObj.getString("user_id"));

        return sb.toString();
    }
}
