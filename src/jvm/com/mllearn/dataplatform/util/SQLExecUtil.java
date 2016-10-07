package com.mllearn.dataplatform.util;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 查询工具类
 * <p/>
 * Author   : wangxp
 * <p/>
 * DateTime : 2015-10-21 13:13
 */
public class SQLExecUtil {
    private static Logger LOGGER = LoggerFactory.getLogger(SQLExecUtil.class);

    /**
     * 执行SQL获取结果集
     *
     * @param connection 执行连接
     * @param sql        执行SQL
     * @param paramMap   分页参数
     *
     * @return 结果集
     */
    @SuppressWarnings("unchecked")
    public static List<Map<String, Object>> executeQuery(Connection connection, String sql, Map<String, Object> paramMap) throws Exception {
        //预处理SQL及设置相应参数
        PreparedStatement statement = null;

        //执行SQL获取结果集
        ResultSet rs = null;
        try {
            //处理SQL参数
            Map<String, Object> sqlHandleResult = replacePoundSignParam(replaceDollarParam(sql, paramMap), paramMap);

            statement = connection.prepareStatement(sqlHandleResult.get("sql").toString());

            List<Object> params = (List<Object>) sqlHandleResult.get("params");

            setSQLParameters(statement, params);

            rs = statement.executeQuery();
            ResultSetMetaData rsMeta = rs.getMetaData();
            int colCount = rsMeta.getColumnCount();

            List<Map<String, Object>> results = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> result = new LinkedHashMap<>();
                for (int i = 1; i <= colCount; i++) {
                    result.put(rsMeta.getColumnLabel(i), rs.getObject(i));
                }

                results.add(result);
            }

            return results;
        } catch (SQLException e) {
            LOGGER.error("执行查询SQL数据集出错");
            throw e;
        } finally {
            try {
                if (null != rs) {
                    rs.close();
                }
            } catch (SQLException e) {
                LOGGER.warn("关闭查询结果集出错");
            }
            try {
                if (null != statement) {
                    statement.close();
                }
            } catch (SQLException e) {
                LOGGER.warn("关闭查询处理语句出错");
            }
        }
    }

    /**
     * 执行SQL获取结果集
     *
     * @param connection 执行连接
     * @param sql        执行SQL
     * @param paramMap   分页参数
     *
     * @return 结果集
     */
    @SuppressWarnings("unchecked")
    public static int executeUpdate(Connection connection, String sql, Map<String, Object> paramMap) throws Exception {
        //预处理SQL及设置相应参数
        PreparedStatement statement = null;
        try {
            //处理SQL参数
            Map<String, Object> sqlHandleResult = replacePoundSignParam(replaceDollarParam(sql, paramMap), paramMap);

            statement = connection.prepareStatement(sqlHandleResult.get("sql").toString());

            List<Object> params = (List<Object>) sqlHandleResult.get("params");

            setSQLParameters(statement, params);

            return statement.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error("执行SQL出错");
            throw e;
        } finally {
            try {
                if (null != statement) {
                    statement.close();
                }
            } catch (SQLException e) {
                LOGGER.warn("关闭查询处理语句出错");
            }
        }
    }

    /**
     * 设置SQL定义参数
     *
     * @param statement 预处理语句
     * @param params    参数
     *
     * @throws Exception 处理异常
     */
    private static void setSQLParameters(PreparedStatement statement, List<Object> params) throws Exception {
        if (CollectionUtils.isNotEmpty(params)) {
            for (int i = 0; i < params.size(); i++) {
                statement.setObject(i + 1, params.get(i));
            }
        }
    }

    /**
     * 替换$符号的预定义参数
     *
     * @param sql      SQL
     * @param paramMap 参数列表
     *
     * @return 替换预定义参数后的SQL
     */
    private static String replaceDollarParam(String sql, Map<String, Object> paramMap) {
        String handleSql = sql;

        Pattern pattern = Pattern.compile("\\$\\{\\s*?(\\w+?)\\s*?\\}");
        Matcher matcher = pattern.matcher(sql);

        while (matcher.find()) {
            String replaceParam = matcher.group();
            String paramName = matcher.group(1);

            handleSql = handleSql.replace(replaceParam, paramMap.get(paramName).toString());
        }

        return handleSql;
    }

    /**
     * 替换#符号的预定义参数
     *
     * @param sql      SQL
     * @param paramMap 参数列表
     *
     * @return 预定义参数列表
     */
    private static Map<String, Object> replacePoundSignParam(String sql, Map<String, Object> paramMap) {
        String handleSql = sql;

        Map<String, Object> result = new HashMap<>();
        List<Object> params = new ArrayList<>();

        Pattern pattern = Pattern.compile("#\\{\\s*?(\\w+?)\\s*?\\}");
        Matcher matcher = pattern.matcher(sql);

        while (matcher.find()) {
            String replaceParam = matcher.group();
            String paramName = matcher.group(1);

            handleSql = handleSql.replace(replaceParam, "?");

            params.add(paramMap.get(paramName));
        }

        result.put("sql", handleSql);
        result.put("params", params);

        return result;
    }
}
