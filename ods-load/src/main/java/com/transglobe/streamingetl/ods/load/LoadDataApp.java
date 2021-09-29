package com.transglobe.streamingetl.ods.load;

import java.sql.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.transglobe.streamingetl.ods.load.bean.DataLoader;
import com.transglobe.streamingetl.ods.load.bean.JbpmVariableinstanceDataLoader;
import com.transglobe.streamingetl.ods.load.bean.TCommisionFeeDataLoader;
import com.transglobe.streamingetl.ods.load.bean.TContractExtendCxDataLoader;
import com.transglobe.streamingetl.ods.load.bean.TContractExtendLogDataLoader;
import com.transglobe.streamingetl.ods.load.bean.TContractProductLogDataLoader;
import com.transglobe.streamingetl.ods.load.bean.TImageDataLoader;
import com.transglobe.streamingetl.ods.load.bean.TPolicyChangeDataLoader;
import com.transglobe.streamingetl.ods.load.bean.TPolicyPrintJobDataLoader;
import com.transglobe.streamingetl.ods.load.bean.TProductCommisionDataLoader;
import com.transglobe.streamingetl.ods.load.bean.TProductionDetailDataLoader;

/**
 * @author oracle
 *
 */
public class LoadDataApp {
	private static final Logger logger = LoggerFactory.getLogger(LoadDataApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";

	public static void main(String[] args) {
		logger.info(">>> start run LoadDataApp");

		String dataDateStr = null; // yyyy-mm-dd
		String tableName= null;
		if (args.length != 0) {
			tableName = args[0];
			dataDateStr = args[1];

		}
		if (tableName != null && dataDateStr != null) {
			logger.info(">>> dataDateStr={}, tableName={}", dataDateStr, tableName);
		}
		
		String profileActive = System.getProperty("profile.active", "");
		logger.info(">>>>>profileActive={}", profileActive);
		
		DataLoader dataloader = null;
		try {
			String configFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;
			Config config = Config.getConfig(configFile);
			Date dataDate = Date.valueOf(dataDateStr);
			String sinkTableName = null;
			boolean updateStatistics = false;
			if (StringUtils.equals(config.sourceTableTCommisionFee, tableName)) {
				dataloader = new TCommisionFeeDataLoader(config, dataDate);
				sinkTableName = config.sinkTableKCommisionFee;
			} else if (StringUtils.equals(config.sourceTableTContractExtendCx, tableName)) {
				dataloader = new TContractExtendCxDataLoader(config, dataDate);
				sinkTableName = config.sinkTableKContractExtendCx;
			} else if (StringUtils.equals(config.sourceTableTContractExtendLog, tableName)) {
				dataloader = new TContractExtendLogDataLoader(config, dataDate);
				sinkTableName = config.sinkTableKContractExtendLog;
				
			}  else if (StringUtils.equals(config.sourceTableTContractProductLog, tableName)) {
				dataloader = new TContractProductLogDataLoader(config, dataDate);
				sinkTableName = config.sinkTableKContractProductLog;
				updateStatistics = true;
				
			} else if (StringUtils.equals(config.sourceTableTImage, tableName)) {
				dataloader = new TImageDataLoader(config, dataDate);
				sinkTableName = config.sinkTableKImage;
				updateStatistics = true;
			} else if (StringUtils.equals(config.sourceTableJbpmVariableinstance, tableName)) {
				dataloader = new JbpmVariableinstanceDataLoader(config, dataDate);
				sinkTableName = config.sinkTableKJbpmVariableinstance;
				updateStatistics = true;
			} else if (StringUtils.equals(config.sourceTableTPolicyChange, tableName)) {
				dataloader = new TPolicyChangeDataLoader(config, dataDate);
				sinkTableName = config.sinkTableKPolicyChange;
			} else if (StringUtils.equals(config.sourceTableTPolicyPrintJob, tableName)) {
				dataloader = new TPolicyPrintJobDataLoader(config, dataDate);
				sinkTableName = config.sinkTableKPolicyPrintJob;
				updateStatistics = true;
			} else if (StringUtils.equals(config.sourceTableTProductCommision, tableName)) {
				dataloader = new TProductCommisionDataLoader(config, dataDate);
				sinkTableName = config.sinkTableKProductCommision;
			} else if (StringUtils.equals(config.sourceTableTProductionDetail, tableName)) {
				dataloader = new TProductionDetailDataLoader(config, dataDate);
				sinkTableName = config.sinkTableKProductionDetail;
				updateStatistics = true;
			} else {
				throw new Exception("No table name match:" + tableName);
			}
			
			logger.info(">>>  Start to drop sink table=");
			dataloader.dropSinkTable();
			
			logger.info(">>>  Start to create sink table");
			dataloader.createSinkTable();
			
			logger.info(">>>  Start to load data");
			dataloader.loadData();
			
			logger.info(">>>  Start: check source and sink data count");
			long sourceCnt = dataloader.getSourceRecordsCount();
			long sinkCnt = dataloader.getSinkRecordsCount();
			if (sourceCnt == sinkCnt) {
				logger.info(">>>  sourceCnt={} equals to sinkCnt={}", sourceCnt, sinkCnt);
			} else {
				throw new Exception("sourceCnt("+ sourceCnt + ") does not equal to sinkCnt(" + sinkCnt + ")");
			}
			
			logger.info(">>>  Start: create sink table Index");	
			dataloader.createSinkTableIndex();
			
			if (updateStatistics) {
				// update statistics
				logger.info(">>>>>DO gatherTableStats");
				dataloader.gatherTableStats(config.sinkTableSchema, sinkTableName);
				
			} else {
				logger.info(">>>>>NO gatherTableStats");
			}

			System.exit(0);
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
