package com.github.hackerwin7.mysql.binlog.tracker.config.impl;

import com.github.hackerwin7.mysql.binlog.tracker.commons.AbstractConf;
import com.github.hackerwin7.mysql.binlog.tracker.commons.constants.CommonConstants;
import com.github.hackerwin7.mysql.binlog.tracker.utils.json.JsonUtils;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;

import java.nio.charset.Charset;
import java.util.HashMap;

/**
 * Created by hp on 4/20/15.
 */
public class JsonConf extends AbstractConf {
    private Logger                                  logger                      = Logger.getLogger(JsonConf.class);

    /**
     *
     * @param job_id
     */
    public JsonConf(String job_id) {
        jobId = job_id;
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void load() throws Exception {
        super.load();
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
        mysqlAddr = jdata.getString("source_host");
        mysqlPort = Integer.valueOf(jdata.getString("source_port"));
        mysqlUsr = jdata.getString("source_user");
        mysqlPsd = jdata.getString("source_password");
        mysqlSlaveId = Long.valueOf(jdata.getString("slaveId"));
        mysqlCharset = Charset.forName(jdata.getString("source_charset"));
        kafkaAcks = jdata.getString("kafka_acks");
        kafkaZk = jdata.getString("kafka_zkserver");
        kafkaRoot = jdata.getString("kafka_zkroot");
        kafkaTopic = jdata.getString("tracker_log_topic");
        monitorZk = jdata.getString("monitor_server");
        monitorRoot = jdata.getString("monitor_zkroot");
        monitorTopic = jdata.getString("monitor_topic");
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
        offsetZk = jdata.getString("offset_zkserver");
        offsetRoot = jdata.getString("offset_zkroot");
        if(jdata.containsKey("db_tab_meta")) {
            if(filterMap == null) {
                filterMap = new HashMap<String, String>();
            }
            if(senseMap == null) {
                senseMap = new HashMap<String, String>();
            }
            filterMap.clear();
            senseMap.clear();
            JSONArray jfa = jdata.getJSONArray("db_tab_meta");
            for (int i = 0; i <= jfa.size() - 1; i++) {
                JSONObject jfao = jfa.getJSONObject(i);
                //filter db and table
                filterMap.put(jfao.getString("dbname") + "." + jfao.getString("tablename"), jfao.getString("tablename"));
                //filter fields
                if(jfao.containsKey("sensefields")) {
                    String[] fields = jfao.getString("sensefields").split(",");
                    for(String field : fields) {
                        senseMap.put(jfao.getString("dbname") + "." + jfao.getString("tablename") + "." + field, field);
                    }
                }
            }
        } else {
            if (filterMap == null) {
                filterMap = new HashMap<String, String>();
            } else {
                filterMap.clear();
            }
            if(senseMap == null) {
                senseMap = new HashMap<String, String>();
            } else {
                senseMap.clear();
            }
        }
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void clear() throws Exception {
        super.clear();
    }

    @Override
    public void statis() throws Exception {
        super.statis();
    }

}
