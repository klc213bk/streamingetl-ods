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
			
			logger.info(">>>  Start: insert 1st T_SUPPL_LOG_SYNC");
			dataloader.insertSupplLogSync();
			
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
	
	private long deleteAndInsertLogminerScn() throws Exception {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "";
		long currentScn = 0L;
		try {
			Class.forName(config.logminerDbDriver);
			conn = DriverManager.getConnection(config.logminerDbUrl, config.logminerDbUsername, config.logminerDbPassword);
			conn.setAutoCommit(false);

			sql = "delete from " + config.logminerTableLogminerScn 
					+ " where STREAMING_NAME=?";	
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, config.streamingName);
			pstmt.executeUpdate();
			pstmt.close();

			sql = "select CURRENT_SCN from gv$database";
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				currentScn = rs.getLong("CURRENT_SCN");
			}
			rs.close();
			pstmt.close();

			long t = System.currentTimeMillis();
			sql = "insert into " + config.logminerTableLogminerScn 
					+ " (STREAMING_NAME,PREV_SCN,SCN,SCN_INSERT_TIME,SCN_UPDATE_TIME) "
					+ " values (?,?,?,?,?)";

			long prevScn = 0L;
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, config.streamingName);
			pstmt.setLong(2, prevScn);
			pstmt.setLong(3, currentScn);
			pstmt.setTimestamp(4, new Timestamp(t));
			pstmt.setTimestamp(5, new Timestamp(t));

			pstmt.executeUpdate();

			conn.commit();
			pstmt.close();

			logger.info("insert into {} with STREAMING_NAME={}, prevscn={}, scn={}", 
					config.logminerTableLogminerScn, config.streamingName, prevScn, currentScn);

		} catch (Exception e) {
			conn.rollback();
			throw e;
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (conn != null) conn.close();

		}
		return currentScn;
	}

}
