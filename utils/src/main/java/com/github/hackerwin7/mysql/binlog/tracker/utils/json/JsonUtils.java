package com.github.hackerwin7.mysql.binlog.tracker.utils.json;

import net.sf.json.JSONObject;

import java.io.InputStreamReader;
import java.net.URL;

/**
 * Created by hp on 4/20/15.
 */
public class JsonUtils {
    /**
     *
     * @param urlStr
     * @return
     * @throws Exception
     */
    public static String loadURL(String urlStr) throws Exception {
        StringBuffer sb = new StringBuffer();
        URL url = new URL(urlStr);
        InputStreamReader isr = new InputStreamReader(url.openStream());
        char[] buffer = new char[1024];
        int len = 0;
        while ((len = isr.read(buffer)) != -1) {
            sb.append(buffer,0,len);
        }
        return sb.toString();
    }

    /**
     *
     * @param jstr
     * @return
     * @throws Exception
     */
    public static JSONObject valueOf(String jstr) throws Exception {
        return JSONObject.fromObject(jstr);
    }

    /**
     *
     * @param url
     * @return
     * @throws Exception
     */
    public static JSONObject url2Json(String url) throws Exception {
        return valueOf(loadURL(url));
    }
}
