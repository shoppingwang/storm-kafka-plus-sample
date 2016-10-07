package com.mllearn.dataplatform.util;

import java.io.IOException;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;

/**
 * 日志配置文件加载
 * <p/>
 * Author   : wangxp
 * <p/>
 * DateTime : 2015-11-09 10:27
 */
public class LogBackConfigLoader {
    public static void load(String configFileLocation) throws IOException, JoranException {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();

        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(LogBackConfigLoader.class.getClassLoader().getResourceAsStream(configFileLocation));
        StatusPrinter.printInCaseOfErrorsOrWarnings(lc);
    }
}
