package com.mllearn.dataplatform.util;

import com.mllearn.dataplatform.mark.ConfigPropertiesHolderSet;
import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 获取分布式锁
 * <p/>
 * Author   : wangxp
 * <p/>
 * DateTime : 2015-11-07 17:33
 */
public class CuratorLockAcquire implements ConfigPropertiesHolderSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(CuratorLockAcquire.class);

    private CuratorFramework client = null;

    private ConfigPropertiesHolder propertiesHolder;

    public CuratorLockAcquire() {}

    public CuratorLockAcquire(ConfigPropertiesHolder propertiesHolder) {
        this.propertiesHolder = propertiesHolder;
    }

    public synchronized CuratorFramework getCuratorClient() {
        if (client == null) {
            try {
                String zkHostPort = propertiesHolder.getStringValue("curator.lock.zk.connect");
                RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
                client = CuratorFrameworkFactory.newClient(zkHostPort, retryPolicy);
                client.start();
            } catch (Exception e) {
                LOGGER.error("Get curatorClient error.", e);
            }
        }

        return client;
    }

    @Override
    public void setConfigPropertiesHolder(ConfigPropertiesHolder config) {
        this.propertiesHolder = config;
    }
}
