package com.transglobe.streamingetl.ods.load;

import java.sql.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.transglobe.streamingetl.ods.load.bean.Config;
import com.transglobe.streamingetl.ods.load.bean.DataLoader;
import com.transglobe.streamingetl.ods.load.bean.TCommisionFeeDataLoader;
import com.transglobe.streamingetl.ods.load.bean.TContractExtendCxDataLoader;
import com.transglobe.streamingetl.ods.load.bean.TImageDataLoader;
import com.transglobe.streamingetl.ods.load.bean.TPolicyPrintJobDataLoader;
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
			if (StringUtils.equals(config.sourceTableTCommisionFee, tableName)) {
				dataloader = new TCommisionFeeDataLoader(config, dataDate);
				dataloader.run();
				
			} else if (StringUtils.equals(config.sourceTableTContractExtendCx, tableName)) {
				dataloader = new TContractExtendCxDataLoader(config, dataDate);
				dataloader.run();
				
			} else if (StringUtils.equals(config.sourceTableTImage, tableName)) {
				dataloader = new TImageDataLoader(config, dataDate);
				dataloader.run();
				
				// update statistics
				logger.info(">>>>>gatherTableStats");
				dataloader.gatherTableStats(config.sinkTableSchema, config.sinkTableKImage);
				
			} else if (StringUtils.equals(config.sourceTableTPolicyPrintJob, tableName)) {
				dataloader = new TPolicyPrintJobDataLoader(config, dataDate);
				dataloader.run();
				
				// update statistics
				logger.info(">>>>>gatherTableStats");
				dataloader.gatherTableStats(config.sinkTableSchema, config.sinkTableKPolicyPrintJob);
			} else if (StringUtils.equals(config.sourceTableTProductionDetail, tableName)) {
				dataloader = new TProductionDetailDataLoader(config, dataDate);
				dataloader.run();
				
				// update statistics
				logger.info(">>>>>gatherTableStats");
				dataloader.gatherTableStats(config.sinkTableSchema, config.sinkTableKProductionDetail);
			} else {
				throw new Exception("No table name match:" + tableName);
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
