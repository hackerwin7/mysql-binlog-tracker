package com.github.hackerwin7.mysql.binlog.tracker.utils.file;

import com.github.hackerwin7.mysql.binlog.tracker.commons.constants.CommonConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.InputStream;

/**
 * Created by hp on 4/20/15.
 */
public class FileUtils {
    private static Logger                           logger                      = Logger.getLogger(FileUtils.class);

    public static InputStream file2in(String filename, String prop) throws Exception {
        String cnf = System.getProperty(prop, CommonConstants.CLASSPATH_COLON + filename);
        logger.info("load file :" + cnf);
        InputStream in = null;
        if(cnf.startsWith(CommonConstants.CLASSPATH_COLON)) {
            cnf = StringUtils.substringAfter(cnf, CommonConstants.CLASSPATH_COLON);
            in = FileUtils.class.getClassLoader().getResourceAsStream(cnf);
        } else {
            in = new FileInputStream(cnf);
        }
        return in;
    }
}
