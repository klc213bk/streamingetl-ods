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

import com.transglobe.streamingetl.ods.load.bean.ContractProductLogBean;
import com.transglobe.streamingetl.ods.load.bean.ProductionDetailBean;

/**
 * @author oracle
 *
 */
public class InitialLoadApp {
	private static final Logger logger = LoggerFactory.getLogger(InitialLoadApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";
	
	private static final String CREATE_TABLE_FILE_NAME_COMMISION_FEE = "createtable-K_COMMISION_FEE.sql";
	private static final String CREATE_TABLE_FILE_NAME_CONTRACT_EXTEND_CX_ = "createtable-K_CONTRACT_EXTEND_CX.sql";
	private static final String CREATE_TABLE_FILE_NAME_CONTRACT_EXTEND_LOG = "createtable-K_CONTRACT_EXTEND_LOG.sql";
	private static final String CREATE_TABLE_FILE_NAME_CONTRACT_PRODUCT_LOG = "createtable-K_CONTRACT_PRODUCT_LOG.sql";
	private static final String CREATE_TABLE_FILE_NAME_IMAGE = "createtable-K_IMAGE.sql";
	private static final String CREATE_TABLE_FILE_NAME_JBPM_VARIABLEINSTANCE = "createtable-K_JBPM_VARIABLEINSTANCE.sql";
	private static final String CREATE_TABLE_FILE_NAME_POLICY_CHANGE = "createtable-K_POLICY_CHANGE.sql";
	private static final String CREATE_TABLE_FILE_NAME_POLICY_PRINT_JOB = "createtable-K_POLICY_PRINT_JOB.sql";
	private static final String CREATE_TABLE_FILE_NAME_PRODUCT_COMMISION = "createtable-K_PRODUCT_COMMISION.sql";
	private static final String CREATE_TABLE_FILE_NAME_PRODUCTION_DETAIL = "createtable-K_PRODUCTION_DETAIL.sql";
	
	private static final String CREATE_SUPPL_LOG_SYNC_TABLE_FILE_NAME = "createtable-T_SUPPL_LOG_SYNC.sql";

	private static final int THREADS = 15;

	//	private static final long SEQ_INTERVAL = 1000000L;

	private BasicDataSource sourceConnectionPool;
	private BasicDataSource sinkConnectionPool;

	public static class LoadBean {
		public String tableName;
		public String sinkTableName;
		public String selectSql;
		public Long startSeq;
		public Long endSeq;
	}
	private Config config;

	public InitialLoadApp(String fileName) throws Exception {
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
	}
	public static void main(String[] args) {
		logger.info(">>> start run InitialLoadApp");

		boolean noload = false;
		if (args.length != 0 && StringUtils.equals("noload", args[0])) {
			noload = true;
		}


		Long t0 = System.currentTimeMillis();

		String profileActive = System.getProperty("profile.active", "");
		logger.info(">>>>>profileActive={}", profileActive);
		try {
			String configFile = StringUtils.isBlank(profileActive)? CONFIG_FILE_NAME : profileActive + "/" + CONFIG_FILE_NAME;

			InitialLoadApp app = new InitialLoadApp(configFile);

			// create sink table
			logger.info(">>>  Start: dropTable");
			app.dropTable(app.config.sinkTableCommisionFee);
			app.dropTable(app.config.sinkTableContractExtendCx);
			app.dropTable(app.config.sinkTableContractExtendLog);
			app.dropTable(app.config.sinkTableContractProductLog);
			app.dropTable(app.config.sinkTableImage);
			app.dropTable(app.config.sinkTableJbpmVariableinstance);
			app.dropTable(app.config.sinkTablePolicyChange);
			app.dropTable(app.config.sinkTablePolicyPrintJob);
			app.dropTable(app.config.sinkTableProductCommision);
			app.dropTable(app.config.sinkTableProductionDetail);
		
			app.dropTable(app.config.sinkTableSupplLogSync);
			logger.info(">>>  End: dropTable DONE!!!");

			logger.info(">>>  Start: createTable");			
			app.createTable(CREATE_TABLE_FILE_NAME_COMMISION_FEE);
			app.createTable(CREATE_TABLE_FILE_NAME_CONTRACT_EXTEND_CX_);
			app.createTable(CREATE_TABLE_FILE_NAME_CONTRACT_EXTEND_LOG);
			app.createTable(CREATE_TABLE_FILE_NAME_CONTRACT_PRODUCT_LOG);
			app.createTable(CREATE_TABLE_FILE_NAME_IMAGE);
			app.createTable(CREATE_TABLE_FILE_NAME_JBPM_VARIABLEINSTANCE);
			app.createTable(CREATE_TABLE_FILE_NAME_POLICY_CHANGE);
			app.createTable(CREATE_TABLE_FILE_NAME_POLICY_PRINT_JOB);
			app.createTable(CREATE_TABLE_FILE_NAME_PRODUCT_COMMISION);
			app.createTable(CREATE_TABLE_FILE_NAME_PRODUCTION_DETAIL);
			
			app.createTable(CREATE_SUPPL_LOG_SYNC_TABLE_FILE_NAME);
			logger.info(">>>  End: createTable DONE!!!");

			// insert  T_LOGMINER_SCN
			logger.info(">>>  Start: insert T_LOGMINER_SCN");
			long currentScn = app.deleteAndInsertLogminerScn();
			logger.info(">>>  End: insert T_LOGMINER_SCN");

			// insert  sink T_SUPPL_LOG_SYNC
			logger.info(">>>  Start: insert T_SUPPL_LOG_SYNC");
			app.insertSupplLogSync(currentScn);
			logger.info(">>>  End: insert T_SUPPL_LOG_SYNC");

			logger.info("init tables span={}, ", (System.currentTimeMillis() - t0));						

			if (!noload) {
			//	app.run();

				//	app.runTLog();
			}
			logger.info("run load data span={}, ", (System.currentTimeMillis() - t0));

			// create indexes
			//app.runCreateIndexes();


			app.close();


			logger.info("total load span={}, ", (System.currentTimeMillis() - t0));


			System.exit(0);

		} catch (Exception e) {
			logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			System.exit(1);
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
	private void insertSupplLogSync(Long currentScn) throws Exception {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "";
		try {
			Class.forName(config.sinkDbDriver);
			conn = DriverManager.getConnection(config.sinkDbUrl);
			conn.setAutoCommit(false);

			long t = System.currentTimeMillis();
			sql = "insert into " + config.sinkTableSupplLogSync 
					+ " (RS_ID, SSN, SCN, REMARK, INSERT_TIME) "
					+ " values (?,?,?,?,?)";

			String rsId = "RS_ID-" + "Start";
			Long ssn = 0L;
			pstmt = conn.prepareStatement(sql);
			pstmt.setString(1, "RS_ID-" + "Start");
			pstmt.setLong(2, ssn);
			pstmt.setLong(3, currentScn);
			pstmt.setString(4, null);
			pstmt.setLong(5, t);

			pstmt.executeUpdate();

			conn.commit();
			pstmt.close();

			logger.info("insert into {} with RS_ID={}, SSN={}, scn={}, INSERT_TIME", 
					config.sinkTableSupplLogSync, rsId, ssn, currentScn, t);

		} catch (Exception e) {
			conn.rollback();
			throw e;
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (conn != null) conn.close();

		}
	}
	private Map<String, String> loadData(LoadBean loadBean){
		Map<String, String> map = new HashMap<>();
		if (StringUtils.equals(config.sourceTableContractProductLog, loadBean.tableName)) {
			map = ContractProductLogBean.loadToSinkTable(loadBean, sourceConnectionPool, sinkConnectionPool);
		} else if (StringUtils.equals(config.sourceTableProductionDetail, loadBean.tableName)) {
			map = ProductionDetailBean.loadToSinkTable(loadBean, sourceConnectionPool, sinkConnectionPool);
		}

		return map;
	}

	
	private void run() throws Exception {

		ExecutorService executor = Executors.newFixedThreadPool(THREADS);

		String sql = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {

			Connection sourceConn = this.sourceConnectionPool.getConnection();
			List<Map<String, String>> tablemapList = new ArrayList<>();

			Map<String, String> tablemap1 = new HashMap<>();
			tablemap1.put("TABLE_NAME", this.config.sourceTableContractProductLog);
			tablemap1.put("SINK_TABLE_NAME", this.config.sinkTableContractProductLog);
			tablemap1.put("SELECT_MIN_ID_SQL", "select min(LOG_ID) as MIN_ID from " + this.config.sourceTableContractProductLog);
			tablemap1.put("SELECT_MAX_ID_SQL", "select max(LOG_ID) as MAX_ID from " + this.config.sourceTableContractProductLog);
			tablemap1.put("SELECT_SQL", "select a.* from " + this.config.sourceTableContractProductLog + " a where ? <= a.log_id and a.log_id < ?");
			tablemap1.put("COUNT_SQL", "select count(*) as CNT from " + this.config.sourceTableContractProductLog + " a where ? <= a.log_id and a.log_id < ?");
			tablemapList.add(tablemap1);

			Map<String, String> tablemap2 = new HashMap<>();
			tablemap2.put("TABLE_NAME", this.config.sourceTableProductionDetail);
			tablemap2.put("SINK_TABLE_NAME", this.config.sinkTableProductionDetail);
			tablemap2.put("SELECT_MIN_ID_SQL", "select min(DETAIL_ID) as MIN_ID from " + this.config.sourceTableProductionDetail);
			tablemap2.put("SELECT_MAX_ID_SQL", "select max(DETAIL_ID) as MAX_ID from " + this.config.sourceTableProductionDetail);
			tablemap2.put("SELECT_SQL", "select a.* from " + this.config.sourceTableProductionDetail + " a where ? <= a.detail_id and a.detail_id < ?");
			tablemap2.put("COUNT_SQL", "select count(*) as CNT from " + this.config.sourceTableProductionDetail + " a where ? <= a.detail_id and a.detail_id < ?");
//			tablemapList.add(tablemap2);

			String tableName = null;
			String sinkTableName = null;
			for (Map<String, String> map : tablemapList) {
				tableName = map.get("TABLE_NAME");
				sinkTableName = map.get("SINK_TABLE_NAME");

				String selectSql = map.get("SELECT_SQL");			

				//				sql = "select min(list_id) as MIN_LIST_ID, max(list_id) as MAX_LIST_ID from " 
				//				+ table + " where list_id >= 31000000";

				// select min id
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
				long subStepSize = 10000;
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
						if (cnt < 10000) {
							LoadBean loadBean = new LoadBean();
							loadBean.tableName = tableName;
							loadBean.sinkTableName = sinkTableName;
							loadBean.startSeq = startIndex;
							loadBean.endSeq = endIndex;
							loadBean.selectSql = selectSql;

							loadBeanList.add(loadBean);

							j++;
						} else {

							while (true) {
								LoadBean loadBean = new LoadBean();
								loadBean.tableName = tableName;
								loadBean.sinkTableName = sinkTableName;
								loadBean.startSeq = startIndex + j * subStepSize;
								loadBean.endSeq = startIndex + (j + 1) * subStepSize;
								loadBean.selectSql = selectSql;

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

				logger.info("table={}, minId={}, maxId={}, loadbean size={}, recordCount={}", tableName, minId, maxId, loadBeanList.size(), recordCount);

				List<CompletableFuture<Map<String, String>>> futures = 
						loadBeanList.stream().map(t -> CompletableFuture.supplyAsync(
								() -> {
									//									String sqlStr = "select a.* from " 
									//											+ t.tableName 
									//											+ " a where " + t.startSeq + " <= a.list_id and a.list_id < " + t.endSeq ;
									return loadData(t);
								}
								, executor)
								)
						.collect(Collectors.toList());			

				List<Map<String, String>> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());

			}

		} finally {
			if (executor != null) executor.shutdown();

		}
	}

	private void dropTable(String tableName) throws SQLException {
		Connection conn = sinkConnectionPool.getConnection();

		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
			stmt.close();
		} catch (java.sql.SQLException e) {
			logger.info(">>>  table:" + tableName + " does not exists!!!");
		} finally {
			if (conn != null) { 
				conn.close();
			}
		}
	}
	private void createTable(String createTableFile) throws Exception {
		Connection conn = sinkConnectionPool.getConnection();

		Statement stmt = null;
		ClassLoader loader = Thread.currentThread().getContextClassLoader();	
		try (InputStream inputStream = loader.getResourceAsStream(createTableFile)) {
			String createTableScript = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
			//	logger.info(">>>  createTableScript={}", createTableScript);
			stmt = conn.createStatement();
			stmt.executeUpdate(createTableScript);
		} catch (SQLException | IOException e) {
			if (stmt != null) stmt.close();
			throw e;
		}

		conn.close();

	}

	private Integer createIndex(String sql) {

		Connection sinkConn = null;
		Statement stmt = null;
		int ret = 0;
		try {
			sinkConn = this.sinkConnectionPool.getConnection();
			stmt = sinkConn.createStatement();
			ret = stmt.executeUpdate(sql);

		} catch(Exception e) {
			logger.error(">>>error={}", ExceptionUtils.getStackTrace(e));
		} finally {
			try {
				if (stmt != null) stmt.close();
				if (sinkConn != null) sinkConn.close();
			}	catch(Exception e) {
				logger.error(">>>error={}", ExceptionUtils.getStackTrace(e));
			} 
		}
		return ret;
	}

}
