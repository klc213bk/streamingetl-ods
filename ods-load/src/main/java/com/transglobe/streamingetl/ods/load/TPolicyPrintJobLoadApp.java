package com.transglobe.streamingetl.ods.load;

import java.sql.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.transglobe.streamingetl.ods.load.bean.Config;
import com.transglobe.streamingetl.ods.load.bean.DataLoader;
import com.transglobe.streamingetl.ods.load.bean.TPolicyPrintJobDataLoader;

/**
 * @author oracle
 *
 */
public class TPolicyPrintJobLoadApp {
	private static final Logger logger = LoggerFactory.getLogger(TPolicyPrintJobLoadApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";

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
			Config config = Config.getConfig(configFile);
			Date dataDate = Date.valueOf(dataDateStr);
			dataloader = new TPolicyPrintJobDataLoader(config, dataDate);
			
			dataloader.run();
			
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
