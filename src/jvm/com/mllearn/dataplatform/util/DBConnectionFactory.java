package com.mllearn.dataplatform.util;

import com.mllearn.dataplatform.pojo.DataSourceInfo;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * 数据库连接获取类
 * <p/>
 * Author   : wangxp
 * <p/>
 * DateTime : 2015-10-16 14:14
 */
public class DBConnectionFactory {
    private static Logger LOGGER = LoggerFactory.getLogger(DBConnectionFactory.class);

    private static final Map<String, DataSource> DATASOURCES = new HashMap<>();

    /**
     * 根据数据源定义信息获取连接
     *
     * @return 数据源的连接
     *
     * @throws java.sql.SQLException 获取连接出错
     */
    public static Connection getConnection(DataSourceInfo dataSourceInfo) throws SQLException {
        Connection conn;

        String dataSourceKey = dataSourceInfo.getDsName();
        BasicDataSource ds = (BasicDataSource) DATASOURCES.get(dataSourceKey);
        try {
            if (null == ds) {
                ds = new BasicDataSource();

                ds.setDriverClassName(dataSourceInfo.getDsDriver());
                ds.setUrl(dataSourceInfo.getDsUrl());
                ds.setUsername(dataSourceInfo.getDsUsername());
                ds.setPassword(dataSourceInfo.getDsPasswd());
                ds.setInitialSize(3);
                ds.setMaxActive(100);
                ds.setMaxIdle(30);
                ds.setMaxWait(1000);
                ds.setPoolPreparedStatements(true);
                ds.setDefaultAutoCommit(false);
                ds.setTimeBetweenEvictionRunsMillis(3600000);
                ds.setMinEvictableIdleTimeMillis(3600000);

                DATASOURCES.put(dataSourceKey, ds);
            }
            conn = ds.getConnection();
        } catch (Exception ex) {
            LOGGER.error("获取数据库连接出错", ex);
            DATASOURCES.remove(dataSourceKey);

            try {
                if (null != ds) {
                    ds.close();
                }
            } catch (Exception ed) {
                LOGGER.warn("关闭数据源出错");
            }


            throw ex;
        }

        return conn;
    }
}
