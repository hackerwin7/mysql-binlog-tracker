package com.github.hackerwin7.mysql.binlog.tracker.config.impl.pipe;

import com.github.hackerwin7.mysql.binlog.tracker.commons.constants.CommonConstants;
import com.github.hackerwin7.mysql.binlog.tracker.config.Config;
import com.github.hackerwin7.mysql.binlog.tracker.utils.json.JsonUtils;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.nio.charset.Charset;
import java.util.HashMap;

/**
 * Created by hp on 7/17/15.
 */
public class JsonPipeConf extends Config {

    /**
     * constructor
     * @throws Exception
     */
    public JsonPipeConf() throws Exception {

    }

    /**
     * override the load method
     * @throws Exception
     */
    @Override
    public void load() throws Exception {
        JSONObject jroot = JsonUtils.url2Json(CommonConstants.TRAIN_URL + jobId);
        if(jroot == null) {
            logger.error("load json from url error, the loaded json is null!!!");
            throw new Exception("load json from url error");
        }
        int _code = jroot.getInt("_code");
        if(_code != 0) {
            logger.error("train configuration is error, the _code is not 0");
            throw new Exception("train configuration is error");
        }
        JSONObject jdata = jroot.getJSONObject("data");
        address = jdata.getString("source_host");
        myPort = Integer.valueOf(jdata.getString("source_port"));
        username = jdata.getString("source_user");
        password = jdata.getString("source_password");
        slaveId = Long.valueOf(jdata.getString("slaveId"));
        charset = Charset.forName(jdata.getString("source_charset"));
        kafkaDataAcks = jdata.getString("kafka_acks");
        kafkaDataZk = jdata.getString("kafka_zkserver");
        kafkaDataZkRoot = jdata.getString("kafka_zkroot");
        kafkaDataTopic = jdata.getString("tracker_log_topic");
        checkpointZk = jdata.getString("checkpoint_zk_servers");
        checkpointZkRoot = jdata.getString("checkpoint_zk_servers_root");
        if(jdata.containsKey("position-logfile")) {
            logfile = jdata.getString("position-logfile");
        }
        if(jdata.containsKey("position-offset")) {
            offset = Long.valueOf(jdata.getString("position-offset"));
        }
        if(jdata.containsKey("position-bid")) {
            batchId = Long.valueOf(jdata.getString("position-bid"));
        }
        if(jdata.containsKey("position-iid")) {
            inId = Long.valueOf(jdata.getString("position-iid"));
        }
        if(jdata.containsKey("db_tab_meta")) {
            if(filterMap == null) {
                filterMap = new HashMap<String, String>();
            }
            if(senseFilterMap == null) {
                senseFilterMap = new HashMap<String, String>();
            }
            filterMap.clear();
            senseFilterMap.clear();
            JSONArray jfa = jdata.getJSONArray("db_tab_meta");
            for (int i = 0; i <= jfa.size() - 1; i++) {
                JSONObject jfao = jfa.getJSONObject(i);
                //filter db and table
                filterMap.put(jfao.getString("dbname") + "." + jfao.getString("tablename"), jfao.getString("tablename"));
                //filter fields
                if(jfao.containsKey("sensefields")) {
                    String[] fields = jfao.getString("sensefields").split(",");
                    for(String field : fields) {
                        senseFilterMap.put(jfao.getString("dbname") + "." + jfao.getString("tablename") + "." + field, field);
                    }
                }
            }
        } else {
            if (filterMap == null) {
                filterMap = new HashMap<String, String>();
            } else {
                filterMap.clear();
            }
            if(senseFilterMap == null) {
                senseFilterMap = new HashMap<String, String>();
            } else {
                senseFilterMap.clear();
            }
        }
    }
}
