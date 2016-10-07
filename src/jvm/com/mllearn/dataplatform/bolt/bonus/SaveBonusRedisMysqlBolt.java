package com.mllearn.dataplatform.bolt.bonus;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import com.mllearn.dataplatform.bolt.SaveRedisBolt;
import com.mllearn.dataplatform.pojo.DataSourceInfo;
import com.mllearn.dataplatform.util.CuratorLockAcquire;
import com.mllearn.dataplatform.util.DBConnectionFactory;
import com.mllearn.dataplatform.util.DateTimeUtil;
import com.mllearn.dataplatform.util.SQLExecUtil;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * 保存数据至MYSQL数据库
 * <p/>
 * Author   : wangxp
 * <p/>
 * DateTime : 2015-11-07 15:43
 */
public class SaveBonusRedisMysqlBolt extends SaveRedisBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(SaveBonusRedisMysqlBolt.class);

    private CuratorLockAcquire curatorLockAcquire;

    private DataSourceInfo dataSourceInfo;

    private List<String> cityIds;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);

        buildDataSourceInfo();

        curatorLockAcquire = new CuratorLockAcquire(propertiesHolder);

        cityIds = new ArrayList<>();
        //添加全国城市
        cityIds.add(CITY_ID_FOR_ALL);
        //添加具体城市
        cityIds.addAll(Arrays.asList(propertiesHolder.getStringValue("bonus.cityids").split(",")));

        Timer timer = new Timer();
        timer.schedule(new SaveRedisToMysqlTimer(), nextExecTime(), 5 * 60 * 1000);
    }

    private void buildDataSourceInfo() {
        dataSourceInfo = new DataSourceInfo();

        dataSourceInfo.setDsName(propertiesHolder.getStringValue("bonus.mysql.ds.name"));
        dataSourceInfo.setDsDriver(propertiesHolder.getStringValue("bonus.mysql.ds.jdbc.driver"));
        dataSourceInfo.setDsUsername(propertiesHolder.getStringValue("bonus.mysql.ds.username"));
        dataSourceInfo.setDsPasswd(propertiesHolder.getStringValue("bonus.mysql.ds.passwd"));
        dataSourceInfo.setDsUrl(propertiesHolder.getStringValue("bonus.mysql.ds.jdbc.url"));
    }

    /**
     * 获取下一次五分钟执行时间
     */
    private Date nextExecTime() {
        try {
            Calendar ca = Calendar.getInstance();
            int currentMinute = ca.get(Calendar.MINUTE);
            int currentFiveMinute = currentMinute - (currentMinute % 5);
            ca.set(Calendar.MINUTE, currentFiveMinute);
            ca.add(Calendar.MINUTE, 5);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

            Date execTime = sdf.parse(sdf.format(ca.getTime()));
            LOGGER.debug("SaveRedisToMysql timer next exec time is {}.", execTime);

            return execTime;
        } catch (ParseException e) {
            LOGGER.error("Init SaveRedisToMysql timer error.", e);
        }

        return null;
    }

    private void saveRedisToMysql() {
        String selectSql = "SELECT COUNT(1) as total FROM t_realtime_bonus " + "WHERE dstring=#{dstring}";

        String saveSql = "INSERT INTO t_realtime_bonus "
                + "(dstring, city_id, band_num, band_money, use_num, use_money, use_person_new, use_person_old, create_time) "
                + "VALUES (#{dstring}, #{cityId}, #{bandNum}, #{bandMoney}, #{useNum}, #{useMoney}, #{usePersonNew}, #{usePersonOld}, #{createTime})";

        InterProcessMutex lock = null;
        Connection conn = null;
        boolean autoCommit = true;
        try {
            lock = new InterProcessMutex(curatorLockAcquire.getCuratorClient(), "/locks/bonus");

            if (lock.acquire(30, TimeUnit.SECONDS)) {
                conn = DBConnectionFactory.getConnection(dataSourceInfo);
                autoCommit = conn.getAutoCommit();
                conn.setAutoCommit(false);

                Map<String, Object> paramMap = new HashMap<>();
                paramMap.put("dstring", DateTimeUtil.format(new Date(), DateTimeUtil.DATE_FORMAT_PATTERN_TO_MINUTE));

                List<Map<String, Object>> results = SQLExecUtil.executeQuery(conn, selectSql, paramMap);
                if (CollectionUtils.isNotEmpty(results) && Integer.parseInt(results.get(0).get("total").toString()) > 0) {
                    return;
                }

                for (String cityId : cityIds) {
                    paramMap.put("cityId", cityId);

                    String bandNumStr = redisUtil.getset(BONUS_BIND_NUM_REDIS_KEY_PREFIX + cityId, String.valueOf(0));
                    long bandNum = null == bandNumStr ? 0 : Long.parseLong(bandNumStr);
                    paramMap.put("bandNum", bandNum);

                    String bandMoneyStr = redisUtil.getset(BONUS_BIND_MONEY_REDIS_KEY_PREFIX + cityId, String.valueOf(0));
                    long bandMoney = null == bandMoneyStr ? 0 : Long.parseLong(bandMoneyStr);
                    paramMap.put("bandMoney", bandMoney);

                    String useNumStr = redisUtil.getset(BONUS_USE_NUM_REDIS_KEY_PREFIX + cityId, String.valueOf(0));
                    long useNum = null == useNumStr ? 0 : Long.parseLong(useNumStr);
                    paramMap.put("useNum", useNum);

                    String useMoneyStr = redisUtil.getset(BONUS_USE_MONEY_REDIS_KEY_PREFIX + cityId, String.valueOf(0));
                    long useMoney = null == useMoneyStr ? 0 : Long.parseLong(useMoneyStr);
                    paramMap.put("useMoney", useMoney);

                    String usePersonNewStr = redisUtil.getset(BONUS_USE_PERSON_NEW_REDIS_KEY_PREFIX + cityId, String.valueOf(0));
                    long usePersonNew = null == usePersonNewStr ? 0 : Long.parseLong(usePersonNewStr);
                    paramMap.put("usePersonNew", usePersonNew);

                    String usePersonOldStr = redisUtil.getset(BONUS_USE_PERSON_OLD_REDIS_KEY_PREFIX + cityId, String.valueOf(0));
                    long usePersonOld = null == usePersonOldStr ? 0 : Long.parseLong(usePersonOldStr);
                    paramMap.put("usePersonOld", usePersonOld);

                    redisUtil.del(BONUS_USE_PHONENO_NEW_REDIS_KEY_PREFIX + cityId, BONUS_USE_PHONENO_OLD_REDIS_KEY_PREFIX + cityId);

                    paramMap.put("createTime", new Timestamp(System.currentTimeMillis()));

                    SQLExecUtil.executeUpdate(conn, saveSql, paramMap);
                }

                conn.commit();
            }
        } catch (Exception ex) {
            try {
                if (null != conn) {
                    conn.rollback();
                }
            } catch (SQLException e) {
                LOGGER.error("Rollback bonus data error.", e);
            }
            LOGGER.error("Save bonus data error.", ex);
        } finally {
            try {
                if (null != conn) {
                    conn.setAutoCommit(autoCommit);
                    conn.close();
                }
                if (null != lock) {
                    lock.release();
                }
            } catch (Exception ex) {
                LOGGER.warn("Close bonus connection error.", ex);
            }
        }
    }

    class SaveRedisToMysqlTimer extends TimerTask {
        @Override
        public void run() {
            LOGGER.debug("SaveRedisToMysqlTimer save data to mysql begin.");
            saveRedisToMysql();
            LOGGER.debug("SaveRedisToMysqlTimer save data to mysql end.");
        }
    }
}
