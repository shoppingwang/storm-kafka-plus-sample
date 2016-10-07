package com.mllearn.dataplatform.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

/**
 * 属性获取
 * <p/>
 * Author   : wangxp
 * <p/>
 * DateTime : 2015-11-06 19:40
 */
public class ConfigPropertiesHolder implements Serializable{

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigPropertiesHolder.class);

    private Properties properties;

    public String getStringValue(String key) {
        return properties.getProperty(key);
    }

    public int getIntValue(String key) {
        return Integer.parseInt(properties.getProperty(key));
    }

    public void init(String configPath) {
        InputStream in = null;
        try {
            in = ConfigPropertiesHolder.class.getClassLoader().getResourceAsStream(configPath);
            properties = new Properties();
            properties.load(in);
        } catch (IOException ex) {
            LOGGER.error("Load " + configPath + " file error.", ex);
            System.exit(-1);
        } finally {
            try {
                if (null != in) {
                    in.close();
                }
            } catch (IOException e) {
                LOGGER.error("Close " + configPath + " InputStream error.", e);
            }
        }
    }
}
