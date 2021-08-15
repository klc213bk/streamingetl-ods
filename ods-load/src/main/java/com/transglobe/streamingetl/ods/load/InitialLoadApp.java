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

import com.transglobe.streamingetl.common.util.OracleUtils;
import com.transglobe.streamingetl.ods.load.bean.ContractProductLogBean;
import com.transglobe.streamingetl.ods.load.bean.ProductionDetailBean;
import com.transglobe.streamingetl.ods.load.bean.TImageLoader;
import com.transglobe.streamingetl.ods.load.bean.TPolicyPrintJobLoader;

/**
 * @author oracle
 *
 */
public class InitialLoadApp {
	private static final Logger logger = LoggerFactory.getLogger(InitialLoadApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";

	public static final String SOURCE_TABLE_NAME_COMMISION_FEE = "T_COMMISION_FEE"; 
	public static final String SOURCE_TABLE_NAME_CONTRACT_EXTEND_CX = "T_CONTRACT_EXTEND_CX"; 
	public static final String SOURCE_TABLE_NAME_CONTRACT_EXTEND_LOG = "T_CONTRACT_EXTEND_LOG";
	public static final String SOURCE_TABLE_NAME_CONTRACT_PRODUCT_LOG = "T_CONTRACT_PRODUCT_LOG";
	public static final String SOURCE_TABLE_NAME_IMAGE = "T_IMAGE";
	public static final String SOURCE_TABLE_NAME_JBPM_VARIABLEINSTANCE = "JBPM_VARIABLEINSTANCE";
	public static final String SOURCE_TABLE_NAME_POLICY_CHANGE = "T_POLICY_CHANGE";
	public static final String SOURCE_TABLE_NAME_POLICY_PRINT_JOB = "T_POLICY_PRINT_JOB";
	public static final String SOURCE_TABLE_NAME_PRODUCT_COMMISION= "T_PRODUCT_COMMISION";
	public static final String SOURCE_TABLE_NAME_PRODUCTION_DETAIL = "T_PRODUCTION_DETAIL";
	
	public static final String SINK_TABLE_NAME_COMMISION_FEE = "K_COMMISION_FEE"; 
	public static final String SINK_TABLE_NAME_CONTRACT_EXTEND_CX= "K_CONTRACT_EXTEND_CX";
	public static final String SINK_TABLE_NAME_CONTRACT_EXTEND_LOG = "K_CONTRACT_EXTEND_LOG";
	public static final String SINK_TABLE_NAME_CONTRACT_PRODUCT_LOG = "K_CONTRACT_PRODUCT_LOG";
	public static final String SINK_TABLE_NAME_IMAGE = "K_IMAGE";
	public static final String SINK_TABLE_NAME_JBPM_VARIABLEINSTANCE = "K_JBPM_VARIABLEINSTANCE";
	public static final String SINK_TABLE_NAME_POLICY_CHANGE = "K_POLICY_CHANGE";
	public static final String SINK_TABLE_NAME_POLICY_PRINT_JOB = "K_POLICY_PRINT_JOB";
	public static final String SINK_TABLE_NAME_PRODUCT_COMMISION = "K_PRODUCT_COMMISION";
	public static final String SINK_TABLE_NAME_PRODUCTION_DETAIL = "K_PRODUCTION_DETAIL";
	public static final String SINK_TABLE_NAME_SUPPL_LOG_SYNC = "K_SUPPL_LOG_SYNC";
	
	private static final String CREATE_TABLE_FILE_NAME_COMMISION_FEE = "createtable-K_COMMISION_FEE.sql";
	private static final String CREATE_TABLE_FILE_NAME_CONTRACT_EXTEND_CX = "createtable-K_CONTRACT_EXTEND_CX.sql";
	private static final String CREATE_TABLE_FILE_NAME_CONTRACT_EXTEND_LOG = "createtable-K_CONTRACT_EXTEND_LOG.sql";
	private static final String CREATE_TABLE_FILE_NAME_CONTRACT_PRODUCT_LOG = "createtable-K_CONTRACT_PRODUCT_LOG.sql";
	private static final String CREATE_TABLE_FILE_NAME_IMAGE = "createtable-K_IMAGE.sql";
	private static final String CREATE_TABLE_FILE_NAME_JBPM_VARIABLEINSTANCE = "createtable-K_JBPM_VARIABLEINSTANCE.sql";
	private static final String CREATE_TABLE_FILE_NAME_POLICY_CHANGE = "createtable-K_POLICY_CHANGE.sql";
	private static final String CREATE_TABLE_FILE_NAME_POLICY_PRINT_JOB = "createtable-K_POLICY_PRINT_JOB.sql";
	private static final String CREATE_TABLE_FILE_NAME_PRODUCT_COMMISION = "createtable-K_PRODUCT_COMMISION.sql";
	private static final String CREATE_TABLE_FILE_NAME_PRODUCTION_DETAIL = "createtable-K_PRODUCTION_DETAIL.sql";
	private static final String CREATE_TABLE_FILE_NAME_SUPPL_LOG_SYNC = "createtable-K_SUPPL_LOG_SYNC.sql";
	
	private static final String CREATE_INDEXES_FILE_NAME_COMMISION_FEE = "createindexes-K_COMMISION_FEE.sql";
	private static final String CREATE_INDEXES_FILE_NAME_CONTRACT_EXTEND_CX = "createindexes-K_CONTRACT_EXTEND_CX.sql";
	private static final String CREATE_INDEXES_FILE_NAME_CONTRACT_EXTEND_LOG = "createindexes-K_CONTRACT_EXTEND_LOG.sql";
	private static final String CREATE_INDEXES_FILE_NAME_CONTRACT_PRODUCT_LOG = "createindexes-K_CONTRACT_PRODUCT_LOG.sql";
	private static final String CREATE_INDEXES_FILE_NAME_IMAGE = "createindexes-K_IMAGE.sql";
	private static final String CREATE_INDEXES_FILE_NAME_JBPM_VARIABLEINSTANCE = "createindexes-K_JBPM_VARIABLEINSTANCE.sql";
	private static final String CREATE_INDEXES_FILE_NAME_POLICY_CHANGE = "createindexes-K_POLICY_CHANGE.sql";
	private static final String CREATE_INDEXES_FILE_NAME_POLICY_PRINT_JOB = "createindexes-K_POLICY_PRINT_JOB.sql";
	private static final String CREATE_INDEXES_FILE_NAME_PRODUCT_COMMISION = "createindexes-K_PRODUCT_COMMISION.sql";
	private static final String CREATE_INDEXES_FILE_NAME_PRODUCTION_DETAIL = "createindexes-K_PRODUCTION_DETAIL.sql";
	private static final String CREATE_INDEXES_FILE_NAME_SUPPL_LOG = "createindexes-K_SUPPL_LOG_SYNC.sql";	

	public static final int THREADS = 15;
	
	public static final int BATCH_COMMIT_SIZE = 1000;

	//	private static final long SEQ_INTERVAL = 1000000L;

	private BasicDataSource sourceConnectionPool;
	private BasicDataSource sinkConnectionPool;
	private BasicDataSource logminerConnectionPool;

	public static class LoadBean {
		public int seq;
		public int loadBeanSize;
		public long startTime;
		public String tableName;
		public long startSeq;
		public long endSeq;
		public long span;
		public long count = 0L;
	}
	private Config config;

	private Date dataDate;
	
	public InitialLoadApp(String fileName, String dataDateStr) throws Exception {
		config = Config.getConfig(fileName);

		sourceConnectionPool = new BasicDataSource();

		sourceConnectionPool.setUrl(config.sourceDbUrl);
		sourceConnectionPool.setUsername(config.sourceDbUsername);
		sourceConnectionPool.setPassword(config.sourceDbPassword);
		sourceConnectionPool.setDriverClassName(config.sourceDbDriver);
		sourceConnectionPool.setMaxTotal(THREADS);

		sinkConnectionPool = new BasicDataSource();
		sinkConnectionPool.setUrl(config.sinkDbUrl);
		sinkConnectionPool.setUsername(config.sinkDbUsername);
		sinkConnectionPool.setPassword(config.sinkDbPassword);
		sinkConnectionPool.setDriverClassName(config.sinkDbDriver);
		sinkConnectionPool.setMaxTotal(THREADS);
		
		logminerConnectionPool = new BasicDataSource();
		logminerConnectionPool.setUrl(config.logminerDbUrl);
		logminerConnectionPool.setUsername(config.logminerDbUsername);
		logminerConnectionPool.setPassword(config.logminerDbPassword);
		logminerConnectionPool.setDriverClassName(config.logminerDbDriver);
		logminerConnectionPool.setMaxTotal(THREADS);

		dataDate = Date.valueOf(dataDateStr);
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

		String dataDateStr = null; // yyyy-mm-dd
		if (args.length != 0) {
			dataDateStr = args[0];
		}

		Long t0 = System.currentTimeMillis();
		Long t1 = 0L;
		
		String profileActive = System.getProperty("profile.active", "");
		logger.info(">>>>>profileActive={}", profileActive);

		Connection sinkConn = null;
		InitialLoadApp app = null;
		try {
			String configFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;

			app = new InitialLoadApp(configFile, dataDateStr);

			sinkConn = app.sinkConnectionPool.getConnection();
			
			// create sink table
			logger.info(">>>  Start: dropTable:{}", SINK_TABLE_NAME_COMMISION_FEE);
			OracleUtils.dropTable(SINK_TABLE_NAME_COMMISION_FEE, sinkConn);
			logger.info(">>>  Start: createTable:{}", CREATE_TABLE_FILE_NAME_COMMISION_FEE);			
			OracleUtils.executeScriptFromFile(CREATE_TABLE_FILE_NAME_COMMISION_FEE, sinkConn);

			logger.info(">>>  Start: dropTable:{}", SINK_TABLE_NAME_CONTRACT_EXTEND_CX);
			OracleUtils.dropTable(SINK_TABLE_NAME_CONTRACT_EXTEND_CX, sinkConn);
			logger.info(">>>  Start: createTable:{}", CREATE_TABLE_FILE_NAME_CONTRACT_EXTEND_CX);	
			OracleUtils.executeScriptFromFile(CREATE_TABLE_FILE_NAME_CONTRACT_EXTEND_CX, sinkConn);

			logger.info(">>>  Start: dropTable:{}", SINK_TABLE_NAME_CONTRACT_EXTEND_LOG);
			OracleUtils.dropTable(SINK_TABLE_NAME_CONTRACT_EXTEND_LOG, sinkConn);
			logger.info(">>>  Start: createTable:{}", CREATE_TABLE_FILE_NAME_CONTRACT_EXTEND_LOG);	
			OracleUtils.executeScriptFromFile(CREATE_TABLE_FILE_NAME_CONTRACT_EXTEND_LOG, sinkConn);

			logger.info(">>>  Start: dropTable:{}", SINK_TABLE_NAME_CONTRACT_PRODUCT_LOG);
			OracleUtils.dropTable(SINK_TABLE_NAME_CONTRACT_PRODUCT_LOG, sinkConn);
			logger.info(">>>  Start: createTable:{}", CREATE_TABLE_FILE_NAME_CONTRACT_PRODUCT_LOG);	
			OracleUtils.executeScriptFromFile(CREATE_TABLE_FILE_NAME_CONTRACT_PRODUCT_LOG, sinkConn);

			logger.info(">>>  Start: dropTable:{}", SINK_TABLE_NAME_IMAGE);
			OracleUtils.dropTable(SINK_TABLE_NAME_IMAGE, sinkConn);
			logger.info(">>>  Start: createTable:{}", CREATE_TABLE_FILE_NAME_IMAGE);	
			OracleUtils.executeScriptFromFile(CREATE_TABLE_FILE_NAME_IMAGE, sinkConn);

			logger.info(">>>  Start: dropTable:{}", SINK_TABLE_NAME_JBPM_VARIABLEINSTANCE);
			OracleUtils.dropTable(SINK_TABLE_NAME_JBPM_VARIABLEINSTANCE, sinkConn);
			logger.info(">>>  Start: createTable:{}", CREATE_TABLE_FILE_NAME_JBPM_VARIABLEINSTANCE);	
			OracleUtils.executeScriptFromFile(CREATE_TABLE_FILE_NAME_JBPM_VARIABLEINSTANCE, sinkConn);

			logger.info(">>>  Start: dropTable:{}", SINK_TABLE_NAME_POLICY_CHANGE);
			OracleUtils.dropTable(SINK_TABLE_NAME_POLICY_CHANGE, sinkConn);
			logger.info(">>>  Start: createTable:{}", CREATE_TABLE_FILE_NAME_POLICY_CHANGE);	
			OracleUtils.executeScriptFromFile(CREATE_TABLE_FILE_NAME_POLICY_CHANGE, sinkConn);

			logger.info(">>>  Start: dropTable:{}", SINK_TABLE_NAME_POLICY_PRINT_JOB);
			OracleUtils.dropTable(SINK_TABLE_NAME_POLICY_PRINT_JOB, sinkConn);
			logger.info(">>>  Start: createTable:{}", CREATE_TABLE_FILE_NAME_POLICY_PRINT_JOB);	
			OracleUtils.executeScriptFromFile(CREATE_TABLE_FILE_NAME_POLICY_PRINT_JOB, sinkConn);

			logger.info(">>>  Start: dropTable:{}", SINK_TABLE_NAME_PRODUCT_COMMISION);
			OracleUtils.dropTable(SINK_TABLE_NAME_PRODUCT_COMMISION, sinkConn);
			logger.info(">>>  Start: createTable:{}", CREATE_TABLE_FILE_NAME_PRODUCT_COMMISION);	
			OracleUtils.executeScriptFromFile(CREATE_TABLE_FILE_NAME_PRODUCT_COMMISION, sinkConn);

			logger.info(">>>  Start: dropTable:{}", SINK_TABLE_NAME_PRODUCTION_DETAIL);
			OracleUtils.dropTable(SINK_TABLE_NAME_PRODUCTION_DETAIL, sinkConn);
			logger.info(">>>  Start: createTable:{}", CREATE_TABLE_FILE_NAME_PRODUCTION_DETAIL);	
			OracleUtils.executeScriptFromFile(CREATE_TABLE_FILE_NAME_PRODUCTION_DETAIL, sinkConn);

			logger.info(">>>  Start: dropTable:{}", SINK_TABLE_NAME_SUPPL_LOG_SYNC);
			OracleUtils.dropTable(SINK_TABLE_NAME_SUPPL_LOG_SYNC, sinkConn);
			logger.info(">>>  Start: createTable:{}", CREATE_TABLE_FILE_NAME_SUPPL_LOG_SYNC);	
			OracleUtils.executeScriptFromFile(CREATE_TABLE_FILE_NAME_SUPPL_LOG_SYNC, sinkConn);

			logger.info(">>>  End: dropTable and create table DONE!!!");
			
			// load data
			app.run();
	
//
//
//			// insert  T_LOGMINER_SCN
//			logger.info(">>>  Start: insert T_LOGMINER_SCN");
//			long currentScn = app.deleteAndInsertLogminerScn();
//			logger.info(">>>  End: insert T_LOGMINER_SCN");
//
//			// insert  sink K_SUPPL_LOG_SYNC
//			logger.info(">>>  Start: insert K_SUPPL_LOG_SYNC");
//			app.insertSupplLogSync(currentScn, sinkConn);
//			logger.info(">>>  End: insert T_SUPPL_LOG_SYNC");
//
//			logger.info("init tables span={}, ", (System.currentTimeMillis() - t0));						
//
//			if (!noload) {
//				app.run();
//
//				//	app.runTLog();
//			}
//			logger.info("run load data span={}, ", (System.currentTimeMillis() - t0));

			// create indexes
			logger.info(">>>  Start: createIndex:{}", CREATE_INDEXES_FILE_NAME_COMMISION_FEE);	
			OracleUtils.executeScriptFromFile(CREATE_INDEXES_FILE_NAME_COMMISION_FEE, sinkConn);
			
			logger.info(">>>  Start: createIndex:{}", CREATE_INDEXES_FILE_NAME_CONTRACT_EXTEND_CX);	
			OracleUtils.executeScriptFromFile(CREATE_INDEXES_FILE_NAME_CONTRACT_EXTEND_CX, sinkConn);
			
			logger.info(">>>  Start: createIndex:{}", CREATE_INDEXES_FILE_NAME_CONTRACT_EXTEND_LOG);	
			OracleUtils.executeScriptFromFile(CREATE_INDEXES_FILE_NAME_CONTRACT_EXTEND_LOG, sinkConn);
			
			logger.info(">>>  Start: createIndex:{}", CREATE_INDEXES_FILE_NAME_CONTRACT_PRODUCT_LOG);	
			OracleUtils.executeScriptFromFile(CREATE_INDEXES_FILE_NAME_CONTRACT_PRODUCT_LOG, sinkConn);
			
			logger.info(">>>  Start: createIndex:{}", CREATE_INDEXES_FILE_NAME_IMAGE);	
			OracleUtils.executeScriptFromFile(CREATE_INDEXES_FILE_NAME_IMAGE, sinkConn);
			
			logger.info(">>>  Start: createIndex:{}", CREATE_INDEXES_FILE_NAME_JBPM_VARIABLEINSTANCE);	
			OracleUtils.executeScriptFromFile(CREATE_INDEXES_FILE_NAME_JBPM_VARIABLEINSTANCE, sinkConn);
			
			logger.info(">>>  Start: createIndex:{}", CREATE_INDEXES_FILE_NAME_POLICY_CHANGE);	
			OracleUtils.executeScriptFromFile(CREATE_INDEXES_FILE_NAME_POLICY_CHANGE, sinkConn);
			
			logger.info(">>>  Start: createIndex:{}", CREATE_INDEXES_FILE_NAME_POLICY_PRINT_JOB);	
			OracleUtils.executeScriptFromFile(CREATE_INDEXES_FILE_NAME_POLICY_PRINT_JOB, sinkConn);
			
			logger.info(">>>  Start: createIndex:{}", CREATE_INDEXES_FILE_NAME_PRODUCT_COMMISION);	
			OracleUtils.executeScriptFromFile(CREATE_INDEXES_FILE_NAME_PRODUCT_COMMISION, sinkConn);
			
			logger.info(">>>  Start: createIndex:{}", CREATE_INDEXES_FILE_NAME_PRODUCTION_DETAIL);	
			OracleUtils.executeScriptFromFile(CREATE_INDEXES_FILE_NAME_PRODUCTION_DETAIL, sinkConn);
			
			logger.info(">>>  Start: createIndex:{}", CREATE_INDEXES_FILE_NAME_SUPPL_LOG);	
			OracleUtils.executeScriptFromFile(CREATE_INDEXES_FILE_NAME_SUPPL_LOG, sinkConn);

			logger.info("total load span={}, ", (System.currentTimeMillis() - t0));


			System.exit(0);

		} catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			System.exit(1);
		} finally {
			try {
				if (app != null) app.close();
			} catch (Exception e) {
				logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}
		}

	}
	private LoadBean loadData(LoadBean loadBean, Date dataDate){
		
		if (StringUtils.equals(SOURCE_TABLE_NAME_IMAGE, loadBean.tableName)) {
			loadBean = TImageLoader.transferData(loadBean, sourceConnectionPool, sinkConnectionPool, logminerConnectionPool, dataDate);
		} else if (StringUtils.equals(SOURCE_TABLE_NAME_POLICY_PRINT_JOB, loadBean.tableName)) {
			loadBean = TPolicyPrintJobLoader.transferData(loadBean, sourceConnectionPool, sinkConnectionPool, logminerConnectionPool, dataDate);
		}

		return loadBean;
	}
	
	private void run() throws Exception {

		ExecutorService executor = Executors.newFixedThreadPool(THREADS);

		String sql = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		long t0 = 0L;
		long t1 = 0L;
		try {
			t0 = System.currentTimeMillis();
			
			Connection sourceConn = this.sourceConnectionPool.getConnection();
			List<Map<String, String>> tablemapList = new ArrayList<>();

//			Map<String, String> imageMap = new HashMap<>();
//			imageMap.put("SOURCE_TABLE_NAME", SOURCE_TABLE_NAME_IMAGE);
//			imageMap.put("SINK_TABLE_NAME", SINK_TABLE_NAME_IMAGE);
//			imageMap.put("SELECT_MIN_ID_SQL", TImageLoader.SELECT_MIN_ID_SQL);
//			imageMap.put("SELECT_MAX_ID_SQL", TImageLoader.SELECT_MAX_ID_SQL);
//			imageMap.put("COUNT_SQL", TImageLoader.COUNT_SQL);
//			tablemapList.add(imageMap);
			
			Map<String, String> policyPrintJobMap = new HashMap<>();
			policyPrintJobMap.put("SOURCE_TABLE_NAME", SOURCE_TABLE_NAME_POLICY_PRINT_JOB);
			policyPrintJobMap.put("SINK_TABLE_NAME", SINK_TABLE_NAME_POLICY_PRINT_JOB);
			policyPrintJobMap.put("SELECT_MIN_ID_SQL", TPolicyPrintJobLoader.SELECT_MIN_ID_SQL);
			policyPrintJobMap.put("SELECT_MAX_ID_SQL", TPolicyPrintJobLoader.SELECT_MAX_ID_SQL);
			policyPrintJobMap.put("COUNT_SQL", TPolicyPrintJobLoader.COUNT_SQL);
			tablemapList.add(policyPrintJobMap);

			String tableName = null;
			for (Map<String, String> map : tablemapList) {
				tableName = map.get("SOURCE_TABLE_NAME");
			
				logger.info("select min id");
				sql = map.get("SELECT_MIN_ID_SQL");
				pstmt = sourceConn.prepareStatement(sql);
				rs = pstmt.executeQuery();
				long minId = 0;
				while (rs.next()) {
					minId = rs.getLong("MIN_ID");
				}
				rs.close();
				pstmt.close();
				logger.info("minId={}", minId);

				// select max id
				logger.info("select max id");
				sql = map.get("SELECT_MAX_ID_SQL");
				pstmt = sourceConn.prepareStatement(sql);
				rs = pstmt.executeQuery();
				long maxId = 0;
				while (rs.next()) {
					maxId = rs.getLong("MAX_ID");
				}
				rs.close();
				pstmt.close();
				logger.info("maxId={}", maxId);


				long stepSize = 1000000;
				long subStepSize = BATCH_COMMIT_SIZE; //10000;
				long startIndex = minId;

				String countSql = map.get("COUNT_SQL");
				PreparedStatement cntPstmt = sourceConn.prepareStatement(countSql);
				ResultSet cntRs = null;
				List<LoadBean> loadBeanList = new ArrayList<>();
				long recordCount = 0L;
				while (startIndex <= maxId) {
					long endIndex = startIndex + stepSize;
					cntPstmt.setLong(1, startIndex);
					cntPstmt.setLong(2, endIndex);
					cntRs = cntPstmt.executeQuery();
					Integer cnt = 0;
					if (cntRs.next()) {
						cnt = cntRs.getInt("CNT");
					}
					recordCount += cnt;
					int j = 0;
					if (cnt > 0) {
						if (cnt <= BATCH_COMMIT_SIZE /*10000*/) {
							LoadBean loadBean = new LoadBean();
							loadBean.tableName = tableName;
							loadBean.startSeq = startIndex;
							loadBean.endSeq = endIndex;
					
							loadBeanList.add(loadBean);

							j++;
						} else {

							while (true) {
								LoadBean loadBean = new LoadBean();
								loadBean.tableName = tableName;
								loadBean.startSeq = startIndex + j * subStepSize;
								loadBean.endSeq = startIndex + (j + 1) * subStepSize;
								
								loadBeanList.add(loadBean);

								j++;

								if (loadBean.endSeq == endIndex) {
									break;
								}
							}
						}
					}
					logger.info("count table={}, startIndex= {}, endIndex={}, cnt={}, loadbeans={}", tableName, startIndex, endIndex, cnt, j);

					startIndex = endIndex;

					
				}
				
				for (int k = 0; k < loadBeanList.size(); k++) {
					LoadBean loadBean = loadBeanList.get(k);
					loadBean.seq = (k+1);
					loadBean.loadBeanSize = loadBeanList.size();
					loadBean.startTime = System.currentTimeMillis();
				}
				
				t1 = System.currentTimeMillis();
				logger.info("table={}, minId={}, maxId={}, loadbean size={}, recordCount={}, beforeLoadSpan={}", tableName, minId, maxId, loadBeanList.size(), recordCount, (t1-t0));

				
				List<CompletableFuture<LoadBean>> futures = 
						loadBeanList.stream().map(t -> CompletableFuture.supplyAsync(
								() -> {
									//									String sqlStr = "select a.* from " 
									//											+ t.tableName 
									//											+ " a where " + t.startSeq + " <= a.list_id and a.list_id < " + t.endSeq ;
									return loadData(t, dataDate);
								}
								, executor)
								)
						.collect(Collectors.toList());			

				List<LoadBean> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());

				double totalSpan = 0.0;
				long totalCount = 0L;
				for (LoadBean loadBean : result) {
					totalSpan += loadBean.span; 
					totalCount += loadBean.count;
				}
				logger.info(">>>tableName={}, loadBeanSize={}, totalCount={}, totalSpan={}", tableName, result.size(), totalCount, totalSpan);
			
				// check total count
				Connection sinkConn = this.sinkConnectionPool.getConnection();
				for (Map<String, String> tabmap : tablemapList) {
					String sourceTableName = tabmap.get("SOURCE_TABLE_NAME");
					String sinkTableName = tabmap.get("SINK_TABLE_NAME");
					sql ="SELECT COUNT(*) CNT from " + sourceTableName;
					pstmt = sourceConn.prepareStatement(sql);
					rs = pstmt.executeQuery();
					int sourceCnt = 0;
					while (rs.next()) {
						sourceCnt = rs.getInt("CNT");
					}
					rs.close();
					pstmt.close();
					
					sql = "SELECT COUNT(*) CNT from " + sinkTableName;
					pstmt = sinkConn.prepareStatement(sql);
					rs = pstmt.executeQuery();
					int sinkCnt = 0;
					while (rs.next()) {
						sinkCnt = rs.getInt("CNT");
					}
					rs.close();
					pstmt.close();
					
					logger.info("sourceTable={}, sourceCount={}, sinktable={}, sinkCount={}", 
							sourceTableName, sourceCnt, sinkTableName, sinkCnt);
				}
			}

		} finally {
			if (executor != null) executor.shutdown();

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
	private void insertSupplLogSync(Long currentScn, Connection sinkConn) throws Exception {
		
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "";
		try {
			Class.forName(config.sinkDbDriver);
		
			sinkConn.setAutoCommit(false);

			long t = System.currentTimeMillis();
			sql = "insert into " + SINK_TABLE_NAME_SUPPL_LOG_SYNC 
					+ " (RS_ID, SSN, SCN, REMARK, INSERT_TIME) "
					+ " values (?,?,?,?,?)";

			String rsId = "RS_ID-" + "01";
			Long ssn = 0L;
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.setString(1, "RS_ID-" + "Start");
			pstmt.setLong(2, ssn);
			pstmt.setLong(3, currentScn);
			pstmt.setString(4, null);
			pstmt.setLong(5, t);

			pstmt.executeUpdate();

			sinkConn.commit();
			pstmt.close();

			logger.info("insert into {} with RS_ID={}, SSN={}, scn={}, INSERT_TIME", 
					SINK_TABLE_NAME_SUPPL_LOG_SYNC, rsId, ssn, currentScn, t);

		} catch (Exception e) {
			sinkConn.rollback();
			throw e;
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		
		}
	}
	

	
}
