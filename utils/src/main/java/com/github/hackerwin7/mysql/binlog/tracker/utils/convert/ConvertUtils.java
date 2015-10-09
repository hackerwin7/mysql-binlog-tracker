package com.github.hackerwin7.mysql.binlog.tracker.utils.convert;

import net.sf.json.JSONObject;

/**
 * Created by hp on 4/21/15.
 */
public class ConvertUtils {
    public static byte[] String2Bytes(String data) throws Exception {
        return data.getBytes("UTF-8");
    }

    public static String Bytes2String(byte[] data) throws Exception {
        return new String(data,"UTF-8");
    }

    public static JSONObject String2Json(String str) throws Exception {
        return JSONObject.fromObject(str);
    }

    public static String Json2String(JSONObject json) throws Exception {
        return json.toString();
    }
}
