package com.transglobe.streamingetl.ods.load;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.Ignition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.transglobe.streamingetl.common.util.CommonConstants;
import com.transglobe.streamingetl.common.util.OracleUtils;
import com.transglobe.streamingetl.common.util.StreamingEtlUtils;
import com.transglobe.streamingetl.ods.load.bean.ContractProductLogBean;
import com.transglobe.streamingetl.ods.load.bean.LoadBean;
import com.transglobe.streamingetl.ods.load.bean.ProductionDetailBean;
import com.transglobe.streamingetl.ods.load.bean.TImageLoader;

/**
 * @author oracle
 *
 */
public class InitLoadApp {
	private static final Logger logger = LoggerFactory.getLogger(InitLoadApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";
	private static final String STREAMING_ETL_NAME = "ODS";

	private static final String SUPPL_LOG_SYNC_TABLE_NAME = "SUPPL_LOG_SYNC";
	private static final String SUPPL_LOG_SYNC_TABLE_CREATE_FILE_NAME = "createtable-SUPPL_LOG_SYNC.sql";
	private static final String SUPPL_LOG_SYNC_TABLE_INDEX_CREATE_FILE_NAME = "createindexes-SUPPL_LOG_SYNC.sql";;

	private BasicDataSource sourceConnectionPool;
	private BasicDataSource sinkConnectionPool;
	private BasicDataSource logminerConnectionPool;


	private Config config;

	public InitLoadApp(String fileName) throws Exception {
		config = Config.getConfig(fileName);

		sourceConnectionPool = new BasicDataSource();

		sourceConnectionPool.setUrl(config.sourceDbUrl);
		sourceConnectionPool.setUsername(config.sourceDbUsername);
		sourceConnectionPool.setPassword(config.sourceDbPassword);
		sourceConnectionPool.setDriverClassName(config.sourceDbDriver);
		sourceConnectionPool.setMaxTotal(1);

		sinkConnectionPool = new BasicDataSource();
		sinkConnectionPool.setUrl(config.sinkDbUrl);
		sinkConnectionPool.setUsername(config.sinkDbUsername);
		sinkConnectionPool.setPassword(config.sinkDbPassword);
		sinkConnectionPool.setDriverClassName(config.sinkDbDriver);
		sinkConnectionPool.setMaxTotal(1);

		logminerConnectionPool = new BasicDataSource();
		logminerConnectionPool.setUrl(config.logminerDbUrl);
		logminerConnectionPool.setUsername(config.logminerDbUsername);
		logminerConnectionPool.setPassword(config.logminerDbPassword);
		logminerConnectionPool.setDriverClassName(config.logminerDbDriver);
		logminerConnectionPool.setMaxTotal(1);

	}
	private void close() {
		try {
			if (sourceConnectionPool != null) sourceConnectionPool.close();
		} catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
		}
		try {
			if (sinkConnectionPool != null) sinkConnectionPool.close();
		} catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
		}
		try {
			if (logminerConnectionPool != null) logminerConnectionPool.close();
		} catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
		}
	}
	public static void main(String[] args) {
		logger.info(">>> start run InitialLoadApp");

		String profileActive = System.getProperty("profile.active", "");
		logger.info(">>>>>profileActive={}", profileActive);

		Connection sinkConn = null;
		Connection logminerConn = null;

		InitLoadApp app = null;
		try {
			String configFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;

			app = new InitLoadApp(configFile);

			sinkConn = app.sinkConnectionPool.getConnection();
			logminerConn = app.logminerConnectionPool.getConnection();

			// create sink table SUPPL_LOG_SYNC
			boolean tableExists = OracleUtils.checkTableExists(SUPPL_LOG_SYNC_TABLE_NAME, sinkConn);
			if (tableExists) {
				logger.info(">>>  Start: tableExists and dropTable:{}", SUPPL_LOG_SYNC_TABLE_NAME);	
				OracleUtils.dropTable(SUPPL_LOG_SYNC_TABLE_NAME, sinkConn);
			}

			logger.info(">>>  Start: createTable:{}", SUPPL_LOG_SYNC_TABLE_NAME);	
			OracleUtils.executeScriptFromFile(SUPPL_LOG_SYNC_TABLE_CREATE_FILE_NAME, sinkConn);

			logger.info(">>>  Start: createIndex:{}", SUPPL_LOG_SYNC_TABLE_INDEX_CREATE_FILE_NAME);	
			OracleUtils.executeScriptFromFile(SUPPL_LOG_SYNC_TABLE_INDEX_CREATE_FILE_NAME, sinkConn);

			System.exit(0);

		} catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			System.exit(1);
		} finally {
			try {
				if (sinkConn != null) sinkConn.close(); 
				if (logminerConn != null) logminerConn.close();
				if (app != null) app.close();
			} catch (Exception e) {
				logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}
		}

	}


}
