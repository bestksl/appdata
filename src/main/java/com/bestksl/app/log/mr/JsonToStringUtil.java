package com.bestksl.app.log.mr;

import com.alibaba.fastjson.JSONObject;

/**
 * @author HaoxuanLi  Github:bestksl
 * @version created date：2019-10-29 18:32
 */
class JsonToStringUtil {
    static String toString(JSONObject jsonObj) {


        return jsonObj.getString("sdk_ver") + "\001" +
                jsonObj.getString("time_zone") + "\001" +
                jsonObj.getString("commit_id") + "\001" +         //提交请求的序号
                jsonObj.getString("commit_time") + "\001" +
                jsonObj.getString("pid") + "\001" +
                jsonObj.getString("app_token") + "\001" +        //app名称：
                jsonObj.getString("app_id") + "\001" +         //app的id标识(所属的公司事业部)
                jsonObj.getString("device_id") + "\001" +
                jsonObj.getString("device_id_type") + "\001" +
                jsonObj.getString("release_channel") + "\001" +    //用户下载该app时所用的app应用市场：360，安智市场，
                jsonObj.getString("app_ver_name") + "\001" +
                jsonObj.getString("app_ver_code") + "\001" +
                jsonObj.getString("os_name") + "\001" +         //操作系统名称
                jsonObj.getString("os_ver") + "\001" +
                jsonObj.getString("language") + "\001" +     //用户的操作系统语言（）
                jsonObj.getString("country") + "\001" +
                jsonObj.getString("manufacture") + "\001" +          //手机生产厂家
                jsonObj.getString("device_model") + "\001" +
                jsonObj.getString("resolution") + "\001" +
                jsonObj.getString("net_type") + "\001" +
                jsonObj.getString("account") + "\001" +
                jsonObj.getString("app_device_id") + "\001" +
                jsonObj.getString("mac") + "\001" +
                jsonObj.getString("android_id") + "\001" +
                jsonObj.getString("imei") + "\001" +
                jsonObj.getString("cid_sn") + "\001" +
                jsonObj.getString("build_num") + "\001" +
                jsonObj.getString("mobile_data_type") + "\001" +
                jsonObj.getString("promotion_channel") + "\001" +
                jsonObj.getString("carrier") + "\001" +
                jsonObj.getString("city") + "\001" +
                jsonObj.getString("user_id");
    }
}
