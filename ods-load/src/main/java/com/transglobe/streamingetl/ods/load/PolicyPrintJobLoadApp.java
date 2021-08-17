package com.transglobe.streamingetl.ods.load;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.transglobe.streamingetl.common.util.StreamingEtlUtils;
import com.transglobe.streamingetl.ods.load.bean.DataLoader;
import com.transglobe.streamingetl.ods.load.bean.TPolicyPrintJobDataLoader;

/**
 * @author oracle
 *
 */
public class PolicyPrintJobLoadApp {
	private static final Logger logger = LoggerFactory.getLogger(PolicyPrintJobLoadApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";

	private Config config;
	
	public static void main(String[] args) {
		logger.info(">>> start run InitialLoadApp");

		String dataDateStr = null; // yyyy-mm-dd
		if (args.length != 0) {
			dataDateStr = args[0];
		}

		String profileActive = System.getProperty("profile.active", "");
		logger.info(">>>>>profileActive={}", profileActive);
		
		DataLoader dataloader = null;
		try {
			String configFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;

			dataloader = new TPolicyPrintJobDataLoader(configFile, dataDateStr);
			
			logger.info(">>>  Start: delete StreamingEtl:{}", dataloader.getStreamingEtlName());	
			dataloader.deleteStreamingEtl();
						
			logger.info(">>>  Start: insert StreamingEtl:{}", dataloader.getStreamingEtlName());	
			dataloader.insertInitStreamingEtl();
	
			logger.info(">>>  Start to update STREAMING_ETL loading start");
			dataloader.updateStreamingEtlLoadingStart();
			
			logger.info(">>>  Start to dropTable");
			dataloader.dropSinkTable();
			
			logger.info(">>>  Start to create sink Table");
			dataloader.createSinkTable();
			
			logger.info(">>>  Start to load data");
			Long t0 = System.currentTimeMillis();
			dataloader.loadData();
			Long t1 = System.currentTimeMillis();
			logger.info(">>>tableName={}, loaddataSpan={}", dataloader.getSourceTableName(), (t1 - t0));

			
			logger.info(">>>  Start: check source and sink data count");
			long sourceCnt = dataloader.getSourceCount();
			long sinkCnt = dataloader.getSinkCount();
			if (sourceCnt == sinkCnt) {
				logger.info(">>>  sourceCnt={} equals to sinkCnt={}", sourceCnt, sinkCnt);
			} else {
				throw new Exception("sourceCnt("+ sourceCnt + ") does not equal to sinkCnt(" + sinkCnt + ")");
			}
			
			logger.info(">>>  Start: createIndex");	
			dataloader.createSinkTableIndex();
			
			logger.info(">>>  Start: insert 1st SUPPL_LOG_SYNC");
			dataloader.insertSupplLogSync();
			
			logger.info(">>>  Start to update STREAMING_ETL loading finish");
			dataloader.updateStreamingEtlLoadingFinish();
			
		} catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			System.exit(1);
		} finally {
			try {
				if (dataloader != null) dataloader.close();
			} catch (Exception e) {
				logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}
		}
	}
	
	
}
