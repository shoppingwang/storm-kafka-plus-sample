package com.mllearn.dataplatform.pojo;

/**
 * 数据源定义
 * <p/>
 * Author   : wangxp
 * <p/>
 * DateTime : 2015-10-15 16:25
 */
public class DataSourceInfo {
    private String dsName;

    private String dsUsername;

    private String dsPasswd;

    private String dsDriver;

    private String dsUrl;

    public String getDsName() {
        return dsName;
    }

    public void setDsName(String dsName) {
        this.dsName = dsName;
    }

    public String getDsUsername() {
        return dsUsername;
    }

    public void setDsUsername(String dsUsername) {
        this.dsUsername = dsUsername;
    }

    public String getDsPasswd() {
        return dsPasswd;
    }

    public void setDsPasswd(String dsPasswd) {
        this.dsPasswd = dsPasswd;
    }

    public String getDsDriver() {
        return dsDriver;
    }

    public void setDsDriver(String dsDriver) {
        this.dsDriver = dsDriver;
    }

    public String getDsUrl() {
        return dsUrl;
    }

    public void setDsUrl(String dsUrl) {
        this.dsUrl = dsUrl;
    }
}
