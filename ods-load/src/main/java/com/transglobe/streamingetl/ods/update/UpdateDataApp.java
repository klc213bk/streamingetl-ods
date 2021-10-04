package com.transglobe.streamingetl.ods.update;

import java.sql.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.transglobe.streamingetl.ods.load.Config;
import com.transglobe.streamingetl.ods.update.bean.JbpmVariableinstanceUpdateDataLoader;
import com.transglobe.streamingetl.ods.update.bean.TCommisionFeeUpdateDataLoader;
import com.transglobe.streamingetl.ods.update.bean.TContractExtendCxUpdateDataLoader;
import com.transglobe.streamingetl.ods.update.bean.TContractExtendLogUpdateDataLoader;
import com.transglobe.streamingetl.ods.update.bean.TContractProductLogUpdateDataLoader;
import com.transglobe.streamingetl.ods.update.bean.TImageUpdateDataLoader;
import com.transglobe.streamingetl.ods.update.bean.TPolicyChangeUpdateDataLoader;
import com.transglobe.streamingetl.ods.update.bean.TPolicyPrintJobUpdateDataLoader;
import com.transglobe.streamingetl.ods.update.bean.TProductCommisionUpdateDataLoader;
import com.transglobe.streamingetl.ods.update.bean.TProductionDetailUpdateDataLoader;
import com.transglobe.streamingetl.ods.update.bean.UpdateDataLoader;

/**
 * @author oracle
 *
 */
public class UpdateDataApp {
	private static final Logger logger = LoggerFactory.getLogger(UpdateDataApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";

	public static void main(String[] args) {
		logger.info(">>> start run LoadDataApp");

		long t0 = System.currentTimeMillis();
		
		String tableName= null;
		String fromUpdateTime = null;
		String toUpdateTime = null;
		if (args.length == 3) {
			tableName = args[0];
			fromUpdateTime = args[1];  // yyyy-mm-dd
			toUpdateTime = args[2];    // yyyy-mm-dd
			logger.info(">>> tableName={}, fromUpdateTimeStr={}, toUpdateTimeStr={}", tableName, args[1], args[2]);

		} else {
			logger.error("Should provide Table Name, fromUpdateTimeStr and toUpdateTimeStr argument");
			System.exit(1);
		}


		String profileActive = System.getProperty("profile.active", "");
		logger.info(">>>>>profileActive={}", profileActive);

		UpdateDataLoader dataloader = null;
		try {
			String configFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;
			Config config = Config.getConfig(configFile);
			if (StringUtils.equals(config.sourceTableTCommisionFee, tableName)) {
				dataloader = new TCommisionFeeUpdateDataLoader(config, fromUpdateTime, toUpdateTime);
			} else if (StringUtils.equals(config.sourceTableTContractExtendCx, tableName)) {
				dataloader = new TContractExtendCxUpdateDataLoader(config, fromUpdateTime, toUpdateTime);
			} else if (StringUtils.equals(config.sourceTableTContractExtendLog, tableName)) {
				dataloader = new TContractExtendLogUpdateDataLoader(config, fromUpdateTime, toUpdateTime);
			}  else if (StringUtils.equals(config.sourceTableTContractProductLog, tableName)) {
				dataloader = new TContractProductLogUpdateDataLoader(config, fromUpdateTime, toUpdateTime);
			} else if (StringUtils.equals(config.sourceTableTImage, tableName)) {
				dataloader = new TImageUpdateDataLoader(config, fromUpdateTime, toUpdateTime);
			} else if (StringUtils.equals(config.sourceTableJbpmVariableinstance, tableName)) {
				dataloader = new JbpmVariableinstanceUpdateDataLoader(config, fromUpdateTime, toUpdateTime);
			} else if (StringUtils.equals(config.sourceTableTPolicyChange, tableName)) {
				dataloader = new TPolicyChangeUpdateDataLoader(config, fromUpdateTime, toUpdateTime);
			} else if (StringUtils.equals(config.sourceTableTPolicyPrintJob, tableName)) {
				dataloader = new TPolicyPrintJobUpdateDataLoader(config, fromUpdateTime, toUpdateTime);
			} else if (StringUtils.equals(config.sourceTableTProductCommision, tableName)) {
				dataloader = new TProductCommisionUpdateDataLoader(config, fromUpdateTime, toUpdateTime);
			} else if (StringUtils.equals(config.sourceTableTProductionDetail, tableName)) {
				dataloader = new TProductionDetailUpdateDataLoader(config, fromUpdateTime, toUpdateTime);
			} else {
				throw new Exception("No table name match:" + tableName);
			}

			logger.info(">>>  Start to drop update table");
			dataloader.dropUpdateTable();

			logger.info(">>>  Start to create update table");
			dataloader.createUpdateTable();

			logger.info(">>>  Start to load update data");
			dataloader.loadData();

			long t1 = System.currentTimeMillis();
			
			logger.info ("update data span={}", (t1 - t0));
			
//			logger.info(">>>  Start: check source and sink data count");
//			long sourceCnt = dataloader.getSourceRecordsCount();
//			long sinkCnt = dataloader.getSinkRecordsCount();
//			if (sourceCnt == sinkCnt) {
//				logger.info(">>>  sourceCnt={} equals to sinkCnt={}", sourceCnt, sinkCnt);
//			} else {
//				throw new Exception("sourceCnt("+ sourceCnt + ") does not equal to sinkCnt(" + sinkCnt + ")");
//			}
//
//			logger.info(">>>  Start: create sink table Index");	
//			dataloader.createSinkTableIndex();
//
//			if (updateStatistics) {
//				// update statistics
//				logger.info(">>>>>DO gatherTableStats");
//				dataloader.gatherTableStats(config.sinkTableSchema, sinkTableName);
//
//			} else {
//				logger.info(">>>>>NO gatherTableStats");
//			}

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
