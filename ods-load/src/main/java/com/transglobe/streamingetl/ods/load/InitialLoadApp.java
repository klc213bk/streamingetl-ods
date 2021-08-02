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

/**
 * @author oracle
 *
 */
public class InitialLoadApp {
	private static final Logger logger = LoggerFactory.getLogger(InitialLoadApp.class);

	private static final String CONFIG_FILE_NAME = "config.properties";
	private static final String CREATE_TABLE_FILE_NAME_PRODUCTION_DETAIL = "createtable-T_PRODUCTION_DETAIL.sql";
	private static final String CREATE_SUPPL_LOG_SYNC_TABLE_FILE_NAME = "createtable-T_SUPPL_LOG_SYNC.sql";

	private static final int THREADS = 15;

	//	private static final long SEQ_INTERVAL = 1000000L;

	private BasicDataSource sourceConnectionPool;
	private BasicDataSource sinkConnectionPool;

	static class LoadBean {
		String tableName;
		String selectSql;
		Long startSeq;
		Long endSeq;
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
			app.dropTable(app.config.sinkTableProductionDetail);
			app.dropTable(app.config.sinkTableSupplLogSync);
			logger.info(">>>  End: dropTable DONE!!!");

			logger.info(">>>  Start: createTable");			
			//			app.createTable(CREATE_TABLE_FILE_NAME_CONTRACT_PRODUCT_LOG);
			app.createTable(CREATE_TABLE_FILE_NAME_PRODUCTION_DETAIL);
			app.createTable(CREATE_SUPPL_LOG_SYNC_TABLE_FILE_NAME);
			logger.info(">>>  End: createTable DONE!!!");

			// insert  T_LOGMINER_SCN
			logger.info(">>>  Start: insert T_LOGMINER_SCN");
			long currentScn = app.insertLogminerScn();
			logger.info(">>>  End: insert T_LOGMINER_SCN");

			// insert  sink T_SUPPL_LOG_SYNC
			logger.info(">>>  Start: insert T_SUPPL_LOG_SYNC");
			app.deleteAndInsertSupplLogSync(currentScn);
			logger.info(">>>  End: insert T_SUPPL_LOG_SYNC");

			logger.info("init tables span={}, ", (System.currentTimeMillis() - t0));						

			if (!noload) {
				app.run();

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
	private long insertLogminerScn() throws Exception {
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
	private void deleteAndInsertSupplLogSync(Long currentScn) throws Exception {
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
		if (StringUtils.equals(config.sourceTableProductionDetail, loadBean.tableName)) {
			map = loadProductionDetail(loadBean);
		}

		return map;
	}

	private Map<String, String> loadProductionDetail(LoadBean loadBean){
		Console cnsl = null;
		Map<String, String> map = new HashMap<>();
		Connection sourceConn = null;
		Connection sinkConn = null;
		PreparedStatement pstmtSource = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = null;
		long t0 = System.currentTimeMillis();
		try {
			sql = loadBean.selectSql;
			sourceConn = this.sourceConnectionPool.getConnection();
			sinkConn = this.sinkConnectionPool.getConnection();

			pstmtSource = sourceConn.prepareStatement(sql);
			pstmtSource.setLong(1, loadBean.startSeq);
			pstmtSource.setLong(2, loadBean.endSeq);
			rs = pstmtSource.executeQuery();

			sinkConn.setAutoCommit(false); 

			pstmt = sinkConn.prepareStatement(
					"insert into " + this.config.sinkTableProductionDetail 
					+ " (DETAIL_ID,PRODUCTION_ID,POLICY_ID,ITEM_ID,PRODUCT_ID"
					+ ",POLICY_YEAR,PRODUCTION_VALUE,EFFECTIVE_DATE,HIERARCHY_DATE,PRODUCER_ID"
					+ ",PRODUCER_POSITION,BENEFIT_TYPE,FEE_TYPE,CHARGE_MODE,PREM_LIST_ID"
					+ ",COMM_ITEM_ID,POLICY_CHG_ID,EXCHANGE_RATE,RELATED_ID,INSURED_ID"
					+ ",POL_PRODUCTION_VALUE,POL_CURRENCY_ID,HIERARCHY_EXIST_INDI,AGGREGATION_ID,PRODUCT_VERSION"
					+ ",SOURCE_TABLE,SOURCE_ID,RESULT_LIST_ID,FINISH_TIME,INSERTED_BY"
					+ ",UPDATED_BY,INSERT_TIME,UPDATE_TIME,INSERT_TIMESTAMP,UPDATE_TIMESTAMP"
					+ ",COMMISSION_RATE,CHEQUE_INDI,PREM_ALLOCATE_YEAR,RECALCULATED_INDI,EXCLUDE_POLICY_INDI"
					+ ",CHANNEL_ORG_ID,AGENT_CATE,YEAR_MONTH,CONVERSION_CATE,ORDER_ID"
					+ ",ASSIGN_RATE,ACCEPT_ID"
					+ ")"
					+ " values (?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?,?,?,?"
					+ " ,?,?,?,?,?,?,?"
					+ ")");

			Long count = 0L;
			while (rs.next()) {
				count++;
				pstmt.setBigDecimal(1, rs.getBigDecimal("DETAIL_ID"));
				pstmt.setBigDecimal(2, rs.getBigDecimal("PRODUCTION_ID"));
				pstmt.setBigDecimal(3, rs.getBigDecimal("POLICY_ID"));
				pstmt.setBigDecimal(4, rs.getBigDecimal("ITEM_ID"));
				pstmt.setBigDecimal(5, rs.getBigDecimal("PRODUCT_ID"));
				pstmt.setBigDecimal(6, rs.getBigDecimal("POLICY_YEAR"));
				pstmt.setBigDecimal(7, rs.getBigDecimal("PRODUCTION_VALUE"));
				pstmt.setTimestamp(8, ((rs.getDate("EFFECTIVE_DATE") == null)? null : new Timestamp(rs.getDate("EFFECTIVE_DATE").getTime())));
				pstmt.setTimestamp(9, ((rs.getDate("HIERARCHY_DATE") == null)? null : new Timestamp(rs.getDate("HIERARCHY_DATE").getTime())));
				pstmt.setBigDecimal(10, rs.getBigDecimal("PRODUCER_ID"));

				pstmt.setString(11, rs.getString("PRODUCER_POSITION"));
				pstmt.setString(12, rs.getString("BENEFIT_TYPE"));
				pstmt.setBigDecimal(13, rs.getBigDecimal("FEE_TYPE"));
				pstmt.setString(14, rs.getString("CHARGE_MODE"));
				pstmt.setBigDecimal(15, rs.getBigDecimal("PREM_LIST_ID"));
				pstmt.setBigDecimal(16, rs.getBigDecimal("COMM_ITEM_ID"));
				pstmt.setBigDecimal(17, rs.getBigDecimal("POLICY_CHG_ID"));
				pstmt.setBigDecimal(18, rs.getBigDecimal("EXCHANGE_RATE"));
				pstmt.setBigDecimal(19, rs.getBigDecimal("RELATED_ID"));
				pstmt.setBigDecimal(20, rs.getBigDecimal("INSURED_ID"));

				pstmt.setBigDecimal(21, rs.getBigDecimal("POL_PRODUCTION_VALUE"));
				pstmt.setBigDecimal(22, rs.getBigDecimal("POL_CURRENCY_ID"));
				pstmt.setString(23, rs.getString("HIERARCHY_EXIST_INDI"));
				pstmt.setBigDecimal(24, rs.getBigDecimal("AGGREGATION_ID"));
				pstmt.setBigDecimal(25, rs.getBigDecimal("PRODUCT_VERSION"));
				pstmt.setString(26, rs.getString("SOURCE_TABLE"));
				pstmt.setBigDecimal(27, rs.getBigDecimal("SOURCE_ID"));
				pstmt.setBigDecimal(28, rs.getBigDecimal("RESULT_LIST_ID"));
				pstmt.setTimestamp(29, ((rs.getDate("FINISH_TIME") == null)? null : new Timestamp(rs.getDate("FINISH_TIME").getTime())));
				pstmt.setBigDecimal(30, rs.getBigDecimal("INSERTED_BY"));

				pstmt.setBigDecimal(31, rs.getBigDecimal("UPDATED_BY"));
				pstmt.setTimestamp(32, ((rs.getDate("INSERT_TIME") == null)? null : new Timestamp(rs.getDate("INSERT_TIME").getTime())));
				pstmt.setTimestamp(33, ((rs.getDate("UPDATE_TIME") == null)? null : new Timestamp(rs.getDate("UPDATE_TIME").getTime())));
				pstmt.setTimestamp(34, ((rs.getDate("INSERT_TIMESTAMP") == null)? null : new Timestamp(rs.getDate("INSERT_TIMESTAMP").getTime())));
				pstmt.setTimestamp(35, ((rs.getDate("UPDATE_TIMESTAMP") == null)? null : new Timestamp(rs.getDate("UPDATE_TIMESTAMP").getTime())));
				pstmt.setBigDecimal(36, rs.getBigDecimal("COMMISSION_RATE"));
				pstmt.setString(37, rs.getString("CHEQUE_INDI"));
				pstmt.setBigDecimal(38, rs.getBigDecimal("PREM_ALLOCATE_YEAR"));
				pstmt.setString(39, rs.getString("RECALCULATED_INDI"));
				pstmt.setString(40, rs.getString("EXCLUDE_POLICY_INDI"));

				pstmt.setBigDecimal(41, rs.getBigDecimal("CHANNEL_ORG_ID"));
				pstmt.setString(42, rs.getString("AGENT_CATE"));
				pstmt.setString(43, rs.getString("YEAR_MONTH"));
				pstmt.setBigDecimal(44, rs.getBigDecimal("CONVERSION_CATE"));
				pstmt.setBigDecimal(45, rs.getBigDecimal("ORDER_ID"));
				pstmt.setBigDecimal(46, rs.getBigDecimal("ASSIGN_RATE"));
				pstmt.setBigDecimal(47, rs.getBigDecimal("ACCEPT_ID"));

				pstmt.addBatch();

				if (count % 3000 == 0) {
					pstmt.executeBatch();//executing the batch  
					sinkConn.commit(); 
					pstmt.clearBatch();
				}
			}

			pstmt.executeBatch();
			if (count > 0) {
				sinkConn.commit(); 
				cnsl = System.console();
				cnsl.printf("   >>>insert into ProductionDetail count=%d, startSeq=%d, endSeq=%d, span=%d \n", count, loadBean.startSeq, loadBean.endSeq, (System.currentTimeMillis() - t0));
				cnsl.flush();
				//				logger.info(">>>>> insert into ProductionDetail count={}, sql={}, startSeq={}, endSeq={}", count, sql, loadBean.startSeq, loadBean.endSeq);
			}


		}  catch (Exception e) {
			map.put("RETURN_CODE", "-999");
			map.put("SQL", sql);
			map.put("SINK_TABLE", this.config.sinkTableProductionDetail);
			map.put("ERROR_MSG", e.getMessage());
			map.put("STACK_TRACE", ExceptionUtils.getStackTrace(e));
			logger.error("message={}, error map={}", e.getMessage(), map);

		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (pstmtSource != null) {
				try {
					pstmtSource.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (sourceConn != null) {
				try {
					sourceConn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if (sinkConn != null) {
				try {
					sinkConn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
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

			//			Map<String, String> tablemap1 = new HashMap<>();
			//			tablemap1.put("TABLE_NAME", this.sourceTableContractProductLog);
			//			tablemap1.put("SELECT_ID_SQL", "select min(LOG_ID) as MIN_ID, max(LOG_ID) as MAX_ID from " + this.sourceTableContractProductLog);
			//			tablemap1.put("SELECT_SQL", "select a.* from " + this.sourceTableContractProductLog + " a where ? <= a.log_id and a.log_id < ?");
			//			tablemap1.put("COUNT_SQL", "select count(*) as CNT from " + this.sourceTableContractProductLog + " a where ? <= a.log_id and a.log_id < ?");
			//			tablemapList.add(tablemap1);

			Map<String, String> tablemap2 = new HashMap<>();
			tablemap2.put("TABLE_NAME", this.config.sourceTableProductionDetail);
			tablemap2.put("SELECT_MIN_ID_SQL", "select min(DETAIL_ID) as MIN_ID from " + this.config.sourceTableProductionDetail);
			tablemap2.put("SELECT_MAX_ID_SQL", "select max(DETAIL_ID) as MAX_ID from " + this.config.sourceTableProductionDetail);
			tablemap2.put("SELECT_SQL", "select a.* from " + this.config.sourceTableProductionDetail + " a where ? <= a.detail_id and a.detail_id < ?");
			tablemap2.put("COUNT_SQL", "select count(*) as CNT from " + this.config.sourceTableProductionDetail + " a where ? <= a.detail_id and a.detail_id < ?");
			tablemapList.add(tablemap2);

			String table = null;
			for (Map<String, String> map : tablemapList) {
				table = map.get("TABLE_NAME");

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
							loadBean.tableName = table;
							loadBean.startSeq = startIndex;
							loadBean.endSeq = endIndex;
							loadBean.selectSql = selectSql;

							loadBeanList.add(loadBean);

							j++;
						} else {

							while (true) {
								LoadBean loadBean = new LoadBean();
								loadBean.tableName = table;
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
					logger.info("count table={}, startIndex= {}, endIndex={}, cnt={}, loadbeans={}", table, startIndex, endIndex, cnt, j);

					startIndex = endIndex;

				}

				logger.info("table={}, minId={}, maxId={}, loadbean size={}, recordCount={}", table, minId, maxId, loadBeanList.size(), recordCount);

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
	//	private Map<String, String> loadContractProductLog(LoadBean loadBean){
	//	//Console cnsl = null;
	//	Map<String, String> map = new HashMap<>();
	//	Connection sourceConn = null;
	//	Connection sinkConn = null;
	//	PreparedStatement pstmtSource = null;
	//	PreparedStatement pstmt = null;
	//	ResultSet rs = null;
	//	String sql = null;
	//	try {
	//		sql = loadBean.selectSql;
	//		sourceConn = this.sourceConnectionPool.getConnection();
	//		sinkConn = this.sinkConnectionPool.getConnection();
	//
	//		pstmtSource = sourceConn.prepareStatement(sql);
	//		pstmtSource.setLong(1, loadBean.startSeq);
	//		pstmtSource.setLong(2, loadBean.endSeq);
	//		rs = pstmtSource.executeQuery();
	//
	//		sinkConn.setAutoCommit(false); 
	//
	//		pstmt = sinkConn.prepareStatement(
	//				"insert into " + this.sinkTableContractProductLog 
	//				+ " (CHANGE_ID,LOG_TYPE,POLICY_CHG_ID,ITEM_ID,MASTER_ID"
	//				+ ",POLICY_ID,PRODUCT_ID,AMOUNT,UNIT,APPLY_DATE"
	//				+ ",EXPIRY_DATE,VALIDATE_DATE,PAIDUP_DATE,LIABILITY_STATE,END_CAUSE"
	//				+ ",INITIAL_TYPE,RENEWAL_TYPE,CHARGE_PERIOD,CHARGE_YEAR,COVERAGE_PERIOD"
	//				+ ",COVERAGE_YEAR,SHORT_END_TIME,EXCEPT_VALUE,BENEFIT_LEVEL,INSURED_CATEGORY"
	//				+ ",SUSPEND,SUSPEND_CAUSE,DERIVATION,PAY_MODE,EXPIRY_CASH_VALUE"
	//				+ ",DECISION_ID,COUNT_WAY,RENEW_DECISION,BONUS_SA,PAY_NEXT" 
	//				+ ",ANNI_BALANCE,FIX_INCREMENT,CPF_COST,CASH_COST,ORIGIN_SA"
	//				+ ",ORIGIN_BONUS_SA,RISK_CODE,EXPOSURE_RATE,REINS_RATE,SUSPEND_CHG_ID"
	//				+ ",NEXT_AMOUNT,WAIVER_START,WAIVER_END,AUTO_PERMNT_LAPSE,PERMNT_LAPSE_NOTICE_DATE"
	//				+ ",WAIVER,WAIVED_SA,ISSUE_AGENT,MASTER_PRODUCT,STRATEGY_CODE"
	//				+ ",LOAN_TYPE,BEN_PERIOD_TYPE,BENEFIT_PERIOD,GURNT_START_DATE,GURNT_PERD_TYPE"
	//				+ ",GURNT_PERIOD,INVEST_HORIZON,MANUAL_SA,DEFER_PERIOD,WAIT_PERIOD"
	//				+ ",NEXT_DISCNTED_PREM_AF,NEXT_POLICY_FEE_AF,NEXT_GROSS_PREM_AF,NEXT_EXTRA_PREM_AF,NEXT_TOTAL_PREM_AF"
	//				+ ",NEXT_STD_PREM_AN,NEXT_DISCNT_PREM_AN,NEXT_DISCNTED_PREM_AN,NEXT_POLICY_FEE_AN,NEXT_EXTRA_PREM_AN"
	//				+ ",WAIV_ANUL_BENEFIT,WAIV_ANUL_PREM,LAPSE_CAUSE,PREM_CHANGE_TIME,SUBMISSION_DATE"
	//				+ ",NEXT_DISCNTED_PREM_BF,NEXT_POLICY_FEE_BF,NEXT_EXTRA_PREM_BF,SA_FACTOR,NEXT_STD_PREM_AF"
	//				+ ",NEXT_DISCNT_PREM_AF,STD_PREM_BF,DISCNT_PREM_BF,DISCNTED_PREM_BF,POLICY_FEE_BF"
	//				+ ",EXTRA_PREM_BF,STD_PREM_AF,DISCNT_PREM_AF,POLICY_FEE_AF,GROSS_PREM_AF"
	//				+ ",EXTRA_PREM_AF,TOTAL_PREM_AF,STD_PREM_AN,DISCNT_PREM_AN,DISCNTED_PREM_AN"
	//				+ ",POLICY_FEE_AN,EXTRA_PREM_AN,NEXT_STD_PREM_BF,NEXT_DISCNT_PREM_BF,DISCNTED_PREM_AF"
	//				+ ",POLICY_PREM_SOURCE,ACTUAL_VALIDATE,ENTITY_FUND,CAR_REG_NO,MANU_SURR_VALUE"
	//				+ ",LOG_ID,ILP_CALC_SA,P_LAPSE_DATE,INITIAL_VALI_DATE,AGE_INCREASE_INDI"
	//				+ ",PAIDUP_OPTION,NEXT_COUNT_WAY,NEXT_UNIT,INSUR_PREM_AF,INSUR_PREM_AN"
	//				+ ",NEXT_INSUR_PREM_AF,NEXT_INSUR_PREM_AN,CAR_REG_NO_START,PHD_PERIOD,ORIGIN_PRODUCT_ID"
	//				+ ",LAST_STATEMENT_DATE,PRE_WAR_INDI,WAIVER_CLAIM_TYPE,ISSUE_DATE,ADVANTAGE_INDI"
	//				+ ",RISK_COMMENCE_DATE,LAST_CMT_FLG,EMS_VERSION,INSERTED_BY,UPDATED_BY"
	//				+ ",INSERT_TIME,UPDATE_TIME,INSERT_TIMESTAMP,UPDATE_TIMESTAMP,LIABILITY_STATE_CAUSE"
	//				+ ",LIABILITY_STATE_DATE,INVEST_SCHEME,TARIFF_TYPE,INDX_SUSPEND_CAUSE,INDX_TYPE"
	//				+ ",INDX_CALC_BASIS,INDX_INDI,COOLING_OFF_OPTION,POSTPONE_PERIOD,NEXT_BENEFIT_LEVEL"
	//				+ ",PRODUCT_VERSION_ID,RENEW_INDI,AGENCY_ORGAN_ID,RELATION_ORGAN_TO_PH,PH_ROLE"
	//				+ ",NHI_INSUR_INDI,VERSION_TYPE_ID,AGREE_READ_INDI,ITEM_ORDER,CUSTOMIZED_PREM"
	//				+ ",SAME_STD_PREM_INDI,PREMIUM_ADJUST_YEARLY,PREMIUM_ADJUST_HALFYEARLY,PREMIUM_ADJUST_QUARTERLY,PREMIUM_ADJUST_MONTHLY"
	//				+ ",PROPOSAL_TERM,COMMISSION_VERSION,INITIAL_SA,OLD_PRODUCT_CODE,COMPAIGN_CODE"
	//				+ ",INITIAL_CHARGE_MODE,CLAIM_STATUS,INITIAL_TOTAL_PREM,OVERWITHDRAW_INDI,PRODUCT_TYPE"
	//				+ ",INSURABILITY_INDICATOR,ETI_SUR,ETI_DAYS,PREMIUM_IS_ZERO_INDI,ILP_INCREASE_RATE"
	//				+ ",SPECIAL_EFFECT_DATE,NSP_AMOUNT,PROPOASL_TERM_TYPE,TOTAL_PAID_PREM,ETI_YEARS"
	//				+ ",LOADING_RATE_POINTER,CANCER_DATE,INSURABILITY_EXTRA_EXECUATE,LAST_CLAIM_DATE,GUARANTEE_OPTION"
	//				+ ",PREM_ALLOC_YEAR_INDI,NAR,HIDE_VALIDATE_DATE_INDI,ETI_SUR_PB,ETA_PAIDUP_AMOUNT"
	//				+ ",ORI_PREMIUM_ADJUST_YEARLY,ORI_PREMIUM_ADJUST_HALFYEARLY,ORI_PREMIUM_ADJUST_QUARTERLY,ORI_PREMIUM_ADJUST_MONTHLY,CLAIM_DISABILITY_RATE"
	//				+ ",HEALTH_INSURANCE_INDI,INSURANCE_NOTICE_INDI,SERVICES_BENEFIT_LEVEL,PAYMENT_FREQ,ISSUE_TYPE"
	//				+ ",LOADING_RATE_POINTER_REASON,RELATION_TO_PH_ROLE,MONEY_DENOMINATED,ORIGIN_PRODUCT_VERSION_ID,TRANSFORM_DATE"
	//				+ ",HEALTH_INSURANCE_VERSION"
	//				+ ")"
	//				+ " values (?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?,?,?,?,?,?,?,?,?,?"
	//				+ " ,?"
	//				+ ")");
	//
	//		Long count = 0L;
	//		while (rs.next()) {
	//			count++;
	//			pstmt.setBigDecimal(1, rs.getBigDecimal("CHANGE_ID"));
	//			pstmt.setString(2, rs.getString("LOG_TYPE"));
	//			pstmt.setBigDecimal(3, rs.getBigDecimal("POLICY_CHG_ID"));
	//			pstmt.setBigDecimal(4, rs.getBigDecimal("ITEM_ID"));
	//			pstmt.setBigDecimal(5, rs.getBigDecimal("MASTER_ID"));
	//			pstmt.setBigDecimal(6, rs.getBigDecimal("POLICY_ID"));
	//			pstmt.setBigDecimal(7, rs.getBigDecimal("PRODUCT_ID"));
	//			pstmt.setBigDecimal(8, rs.getBigDecimal("AMOUNT"));
	//			pstmt.setBigDecimal(9, rs.getBigDecimal("UNIT"));
	//			pstmt.setTimestamp(10, ((rs.getDate("APPLY_DATE") == null)? null : new Timestamp(rs.getDate("APPLY_DATE").getTime())));
	//			pstmt.setTimestamp(11, ((rs.getDate("EXPIRY_DATE") == null)? null : new Timestamp(rs.getDate("EXPIRY_DATE").getTime())));
	//			pstmt.setTimestamp(12, ((rs.getDate("VALIDATE_DATE") == null)? null : new Timestamp(rs.getDate("VALIDATE_DATE").getTime())));
	//			pstmt.setTimestamp(13, ((rs.getDate("PAIDUP_DATE") == null)? null : new Timestamp(rs.getDate("PAIDUP_DATE").getTime())));
	//			pstmt.setBigDecimal(14, rs.getBigDecimal("LIABILITY_STATE"));
	//			pstmt.setBigDecimal(15, rs.getBigDecimal("END_CAUSE"));
	//			pstmt.setString(16, rs.getString("INITIAL_TYPE"));
	//			pstmt.setString(17, rs.getString("RENEWAL_TYPE"));
	//			pstmt.setString(18, rs.getString("CHARGE_PERIOD"));
	//			pstmt.setBigDecimal(19, rs.getBigDecimal("CHARGE_YEAR"));
	//			pstmt.setString(20, rs.getString("COVERAGE_PERIOD"));
	//			pstmt.setBigDecimal(21, rs.getBigDecimal("COVERAGE_YEAR"));
	//			pstmt.setTimestamp(22, ((rs.getDate("SHORT_END_TIME") == null)? null : new Timestamp(rs.getDate("SHORT_END_TIME").getTime())));
	//			pstmt.setBigDecimal(23, rs.getBigDecimal("EXCEPT_VALUE"));
	//			pstmt.setString(24, rs.getString("BENEFIT_LEVEL"));
	//			pstmt.setString(25, rs.getString("INSURED_CATEGORY"));
	//			pstmt.setString(26, rs.getString("SUSPEND"));
	//			pstmt.setBigDecimal(27, rs.getBigDecimal("SUSPEND_CAUSE"));
	//			pstmt.setString(28, rs.getString("DERIVATION"));
	//			pstmt.setInt(29, rs.getInt("PAY_MODE"));
	//			pstmt.setBigDecimal(30, rs.getBigDecimal("EXPIRY_CASH_VALUE"));
	//			pstmt.setBigDecimal(31, rs.getBigDecimal("DECISION_ID"));
	//			pstmt.setString(32, rs.getString("COUNT_WAY"));
	//			pstmt.setString(33, rs.getString("RENEW_DECISION"));
	//			pstmt.setBigDecimal(34, rs.getBigDecimal("BONUS_SA"));
	//			pstmt.setBigDecimal(35, rs.getBigDecimal("PAY_NEXT"));
	//			pstmt.setBigDecimal(36, rs.getBigDecimal("ANNI_BALANCE"));
	//			pstmt.setString(37, rs.getString("FIX_INCREMENT"));
	//			pstmt.setBigDecimal(38, rs.getBigDecimal("CPF_COST"));
	//			pstmt.setBigDecimal(39, rs.getBigDecimal("CASH_COST"));
	//			pstmt.setBigDecimal(40, rs.getBigDecimal("ORIGIN_SA"));
	//			pstmt.setBigDecimal(41, rs.getBigDecimal("ORIGIN_BONUS_SA"));
	//			pstmt.setBigDecimal(42, rs.getBigDecimal("RISK_CODE"));
	//			pstmt.setBigDecimal(43, rs.getBigDecimal("EXPOSURE_RATE"));
	//			pstmt.setBigDecimal(44, rs.getBigDecimal("REINS_RATE"));
	//			pstmt.setBigDecimal(45, rs.getBigDecimal("SUSPEND_CHG_ID"));
	//			pstmt.setBigDecimal(46, rs.getBigDecimal("NEXT_AMOUNT"));
	//			pstmt.setTimestamp(47, ((rs.getDate("WAIVER_START") == null)? null : new Timestamp(rs.getDate("WAIVER_START").getTime())));
	//			pstmt.setTimestamp(48, ((rs.getDate("WAIVER_END") == null)? null : new Timestamp(rs.getDate("WAIVER_END").getTime())));
	//			pstmt.setString(49, rs.getString("AUTO_PERMNT_LAPSE"));
	//			pstmt.setTimestamp(50, ((rs.getDate("PERMNT_LAPSE_NOTICE_DATE") == null)? null : new Timestamp(rs.getDate("PERMNT_LAPSE_NOTICE_DATE").getTime())));
	//			pstmt.setString(51, rs.getString("WAIVER"));
	//			pstmt.setBigDecimal(52, rs.getBigDecimal("WAIVED_SA"));
	//			pstmt.setBigDecimal(53, rs.getBigDecimal("ISSUE_AGENT"));
	//			pstmt.setBigDecimal(54, rs.getBigDecimal("MASTER_PRODUCT"));
	//			pstmt.setString(55, rs.getString("STRATEGY_CODE"));
	//			pstmt.setString(56, rs.getString("LOAN_TYPE"));
	//			pstmt.setString(57, rs.getString("BEN_PERIOD_TYPE"));
	//			pstmt.setBigDecimal(58, rs.getBigDecimal("BENEFIT_PERIOD"));
	//			pstmt.setTimestamp(59, ((rs.getDate("GURNT_START_DATE") == null)? null : new Timestamp(rs.getDate("GURNT_START_DATE").getTime())));
	//			pstmt.setString(60, rs.getString("GURNT_PERD_TYPE"));
	//			pstmt.setBigDecimal(61, rs.getBigDecimal("GURNT_PERIOD"));
	//			pstmt.setBigDecimal(62, rs.getBigDecimal("INVEST_HORIZON"));
	//			pstmt.setString(63, rs.getString("MANUAL_SA"));
	//			pstmt.setBigDecimal(64, rs.getBigDecimal("DEFER_PERIOD"));
	//			pstmt.setBigDecimal(65, rs.getBigDecimal("WAIT_PERIOD"));
	//			pstmt.setBigDecimal(66, rs.getBigDecimal("NEXT_DISCNTED_PREM_AF"));
	//			pstmt.setBigDecimal(67, rs.getBigDecimal("NEXT_POLICY_FEE_AF"));
	//			pstmt.setBigDecimal(68, rs.getBigDecimal("NEXT_GROSS_PREM_AF"));
	//			pstmt.setBigDecimal(69, rs.getBigDecimal("NEXT_EXTRA_PREM_AF"));
	//			pstmt.setBigDecimal(70, rs.getBigDecimal("NEXT_TOTAL_PREM_AF"));
	//			pstmt.setBigDecimal(71, rs.getBigDecimal("NEXT_STD_PREM_AN"));
	//			pstmt.setBigDecimal(72, rs.getBigDecimal("NEXT_DISCNT_PREM_AN"));
	//			pstmt.setBigDecimal(73, rs.getBigDecimal("NEXT_DISCNTED_PREM_AN"));
	//			pstmt.setBigDecimal(74, rs.getBigDecimal("NEXT_POLICY_FEE_AN"));
	//			pstmt.setBigDecimal(75, rs.getBigDecimal("NEXT_EXTRA_PREM_AN"));
	//			pstmt.setBigDecimal(76, rs.getBigDecimal("WAIV_ANUL_BENEFIT"));
	//			pstmt.setBigDecimal(77, rs.getBigDecimal("WAIV_ANUL_PREM"));
	//			pstmt.setBigDecimal(78, rs.getBigDecimal("LAPSE_CAUSE"));
	//			pstmt.setTimestamp(79, ((rs.getDate("PREM_CHANGE_TIME") == null)? null : new Timestamp(rs.getDate("PREM_CHANGE_TIME").getTime())));
	//			pstmt.setTimestamp(80, ((rs.getDate("SUBMISSION_DATE") == null)? null : new Timestamp(rs.getDate("SUBMISSION_DATE").getTime())));
	//			pstmt.setBigDecimal(81, rs.getBigDecimal("NEXT_DISCNTED_PREM_BF"));
	//			pstmt.setBigDecimal(82, rs.getBigDecimal("NEXT_POLICY_FEE_BF"));
	//			pstmt.setBigDecimal(83, rs.getBigDecimal("NEXT_EXTRA_PREM_BF"));
	//			pstmt.setBigDecimal(84, rs.getBigDecimal("SA_FACTOR"));
	//			pstmt.setBigDecimal(85, rs.getBigDecimal("NEXT_STD_PREM_AF"));
	//			pstmt.setBigDecimal(86, rs.getBigDecimal("NEXT_DISCNT_PREM_AF"));
	//			pstmt.setBigDecimal(87, rs.getBigDecimal("STD_PREM_BF"));
	//			pstmt.setBigDecimal(88, rs.getBigDecimal("DISCNT_PREM_BF"));
	//			pstmt.setBigDecimal(89, rs.getBigDecimal("DISCNTED_PREM_BF"));
	//			pstmt.setBigDecimal(90, rs.getBigDecimal("POLICY_FEE_BF"));
	//			pstmt.setBigDecimal(91, rs.getBigDecimal("EXTRA_PREM_BF"));
	//			pstmt.setBigDecimal(92, rs.getBigDecimal("STD_PREM_AF"));
	//			pstmt.setBigDecimal(93, rs.getBigDecimal("DISCNT_PREM_AF"));
	//			pstmt.setBigDecimal(94, rs.getBigDecimal("POLICY_FEE_AF"));
	//			pstmt.setBigDecimal(95, rs.getBigDecimal("GROSS_PREM_AF"));
	//			pstmt.setBigDecimal(96, rs.getBigDecimal("EXTRA_PREM_AF"));
	//			pstmt.setBigDecimal(97, rs.getBigDecimal("TOTAL_PREM_AF"));
	//			pstmt.setBigDecimal(98, rs.getBigDecimal("STD_PREM_AN"));
	//			pstmt.setBigDecimal(99, rs.getBigDecimal("DISCNT_PREM_AN"));
	//			pstmt.setBigDecimal(100, rs.getBigDecimal("POLICY_FEE_AN"));
	//			pstmt.setBigDecimal(101, rs.getBigDecimal("DISCNTED_PREM_AN"));
	//			pstmt.setBigDecimal(102, rs.getBigDecimal("EXTRA_PREM_AN"));
	//			pstmt.setBigDecimal(103, rs.getBigDecimal("NEXT_STD_PREM_BF"));
	//			pstmt.setBigDecimal(104, rs.getBigDecimal("NEXT_DISCNT_PREM_BF"));
	//			pstmt.setBigDecimal(105, rs.getBigDecimal("DISCNTED_PREM_AF"));
	//			pstmt.setString(106, rs.getString("POLICY_PREM_SOURCE"));
	//			pstmt.setTimestamp(107, ((rs.getDate("ACTUAL_VALIDATE") == null)? null : new Timestamp(rs.getDate("ACTUAL_VALIDATE").getTime())));
	//			pstmt.setString(108, rs.getString("ENTITY_FUND"));
	//			pstmt.setString(109, rs.getString("CAR_REG_NO"));
	//			pstmt.setString(110, rs.getString("MANU_SURR_VALUE"));
	//			pstmt.setBigDecimal(111, rs.getBigDecimal("LOG_ID"));
	//			pstmt.setBigDecimal(112, rs.getBigDecimal("ILP_CALC_SA"));
	//			pstmt.setTimestamp(113, ((rs.getDate("P_LAPSE_DATE") == null)? null : new Timestamp(rs.getDate("P_LAPSE_DATE").getTime())));
	//			pstmt.setTimestamp(114, ((rs.getDate("INITIAL_VALI_DATE") == null)? null : new Timestamp(rs.getDate("INITIAL_VALI_DATE").getTime())));
	//			pstmt.setString(115, rs.getString("AGE_INCREASE_INDI"));
	//			pstmt.setString(116, rs.getString("PAIDUP_OPTION"));
	//			pstmt.setString(117, rs.getString("NEXT_COUNT_WAY"));
	//			pstmt.setBigDecimal(118, rs.getBigDecimal("NEXT_UNIT"));
	//			pstmt.setBigDecimal(119, rs.getBigDecimal("INSUR_PREM_AF"));
	//			pstmt.setBigDecimal(120, rs.getBigDecimal("INSUR_PREM_AN"));
	//			pstmt.setBigDecimal(121, rs.getBigDecimal("NEXT_INSUR_PREM_AF"));
	//			pstmt.setBigDecimal(122, rs.getBigDecimal("NEXT_INSUR_PREM_AN"));
	//			pstmt.setTimestamp(123, ((rs.getDate("CAR_REG_NO_START") == null)? null : new Timestamp(rs.getDate("CAR_REG_NO_START").getTime())));
	//			pstmt.setBigDecimal(124, rs.getBigDecimal("PHD_PERIOD"));
	//			pstmt.setBigDecimal(125, rs.getBigDecimal("ORIGIN_PRODUCT_ID"));
	//			pstmt.setTimestamp(126, ((rs.getDate("LAST_STATEMENT_DATE") == null)? null : new Timestamp(rs.getDate("LAST_STATEMENT_DATE").getTime())));
	//			pstmt.setString(127, rs.getString("PRE_WAR_INDI"));
	//			pstmt.setBigDecimal(128, rs.getBigDecimal("WAIVER_CLAIM_TYPE"));
	//			pstmt.setTimestamp(129, ((rs.getDate("ISSUE_DATE") == null)? null : new Timestamp(rs.getDate("ISSUE_DATE").getTime())));
	//			pstmt.setString(130, rs.getString("ADVANTAGE_INDI"));
	//			pstmt.setTimestamp(131, ((rs.getDate("RISK_COMMENCE_DATE") == null)? null : new Timestamp(rs.getDate("RISK_COMMENCE_DATE").getTime())));
	//			pstmt.setString(132, rs.getString("LAST_CMT_FLG"));
	//			pstmt.setBigDecimal(133, rs.getBigDecimal("EMS_VERSION"));
	//			pstmt.setBigDecimal(134, rs.getBigDecimal("INSERTED_BY"));
	//			pstmt.setBigDecimal(135, rs.getBigDecimal("UPDATED_BY"));
	//			pstmt.setTimestamp(136, ((rs.getDate("INSERT_TIME") == null)? null : new Timestamp(rs.getDate("INSERT_TIME").getTime())));
	//			pstmt.setTimestamp(137, ((rs.getDate("UPDATE_TIME") == null)? null : new Timestamp(rs.getDate("UPDATE_TIME").getTime())));
	//			pstmt.setTimestamp(138, ((rs.getDate("INSERT_TIMESTAMP") == null)? null : new Timestamp(rs.getDate("INSERT_TIMESTAMP").getTime())));
	//			pstmt.setTimestamp(139, ((rs.getDate("UPDATE_TIMESTAMP") == null)? null : new Timestamp(rs.getDate("UPDATE_TIMESTAMP").getTime())));
	//			pstmt.setBigDecimal(140, rs.getBigDecimal("LIABILITY_STATE_CAUSE"));
	//			pstmt.setTimestamp(141, ((rs.getDate("LIABILITY_STATE_DATE") == null)? null : new Timestamp(rs.getDate("LIABILITY_STATE_DATE").getTime())));
	//			pstmt.setString(142, rs.getString("INVEST_SCHEME"));
	//			pstmt.setBigDecimal(143, rs.getBigDecimal("TARIFF_TYPE"));
	//			pstmt.setBigDecimal(144, rs.getBigDecimal("INDX_SUSPEND_CAUSE"));
	//			pstmt.setBigDecimal(145, rs.getBigDecimal("INDX_TYPE"));
	//			pstmt.setBigDecimal(146, rs.getBigDecimal("INDX_CALC_BASIS"));
	//			pstmt.setString(147, rs.getString("INDX_INDI"));
	//			pstmt.setBigDecimal(148, rs.getBigDecimal("COOLING_OFF_OPTION"));
	//			pstmt.setBigDecimal(149, rs.getBigDecimal("POSTPONE_PERIOD"));
	//			pstmt.setString(150, rs.getString("NEXT_BENEFIT_LEVEL"));
	//			pstmt.setBigDecimal(151, rs.getBigDecimal("PRODUCT_VERSION_ID"));
	//			pstmt.setString(152, rs.getString("RENEW_INDI"));
	//			pstmt.setString(153, rs.getString("AGENCY_ORGAN_ID"));
	//			pstmt.setString(154, rs.getString("RELATION_ORGAN_TO_PH"));
	//			pstmt.setString(155, rs.getString("PH_ROLE"));
	//			pstmt.setString(156, rs.getString("NHI_INSUR_INDI"));
	//			pstmt.setBigDecimal(157, rs.getBigDecimal("VERSION_TYPE_ID"));
	//			pstmt.setString(158, rs.getString("AGREE_READ_INDI"));
	//			pstmt.setBigDecimal(159, rs.getBigDecimal("ITEM_ORDER"));
	//			pstmt.setBigDecimal(160, rs.getBigDecimal("CUSTOMIZED_PREM"));
	//			pstmt.setString(161, rs.getString("SAME_STD_PREM_INDI"));
	//			pstmt.setBigDecimal(162, rs.getBigDecimal("PREMIUM_ADJUST_YEARLY"));
	//			pstmt.setBigDecimal(163, rs.getBigDecimal("PREMIUM_ADJUST_HALFYEARLY"));
	//			pstmt.setBigDecimal(164, rs.getBigDecimal("PREMIUM_ADJUST_QUARTERLY"));
	//			pstmt.setBigDecimal(165, rs.getBigDecimal("PREMIUM_ADJUST_MONTHLY"));
	//			pstmt.setBigDecimal(166, rs.getBigDecimal("PROPOSAL_TERM"));
	//			pstmt.setBigDecimal(167, rs.getBigDecimal("COMMISSION_VERSION"));
	//			pstmt.setBigDecimal(168, rs.getBigDecimal("INITIAL_SA"));
	//			pstmt.setString(169, rs.getString("OLD_PRODUCT_CODE"));
	//			pstmt.setString(170, rs.getString("COMPAIGN_CODE"));
	//			pstmt.setString(171, rs.getString("INITIAL_CHARGE_MODE"));
	//			pstmt.setString(172, rs.getString("CLAIM_STATUS"));
	//			pstmt.setBigDecimal(173, rs.getBigDecimal("INITIAL_TOTAL_PREM"));
	//			pstmt.setString(174, rs.getString("OVERWITHDRAW_INDI"));
	//			pstmt.setString(175, rs.getString("PRODUCT_TYPE"));
	//			pstmt.setString(176, rs.getString("INSURABILITY_INDICATOR"));
	//			pstmt.setBigDecimal(177, rs.getBigDecimal("ETI_SUR"));
	//			pstmt.setBigDecimal(178, rs.getBigDecimal("ETI_DAYS"));
	//			pstmt.setString(179, rs.getString("PREMIUM_IS_ZERO_INDI"));
	//			pstmt.setBigDecimal(180, rs.getBigDecimal("ILP_INCREASE_RATE"));
	//			pstmt.setTimestamp(181, ((rs.getDate("SPECIAL_EFFECT_DATE") == null)? null : new Timestamp(rs.getDate("SPECIAL_EFFECT_DATE").getTime())));
	//			pstmt.setBigDecimal(182, rs.getBigDecimal("NSP_AMOUNT"));
	//			pstmt.setString(183, rs.getString("PROPOASL_TERM_TYPE"));
	//			pstmt.setBigDecimal(184, rs.getBigDecimal("TOTAL_PAID_PREM"));
	//			pstmt.setBigDecimal(185, rs.getBigDecimal("ETI_YEARS"));
	//			pstmt.setString(186, rs.getString("LOADING_RATE_POINTER"));
	//			pstmt.setTimestamp(187, ((rs.getDate("CANCER_DATE") == null)? null : new Timestamp(rs.getDate("CANCER_DATE").getTime())));
	//			pstmt.setString(188, rs.getString("INSURABILITY_EXTRA_EXECUATE"));
	//			pstmt.setTimestamp(189, ((rs.getDate("LAST_CLAIM_DATE") == null)? null : new Timestamp(rs.getDate("LAST_CLAIM_DATE").getTime())));
	//			pstmt.setString(190, rs.getString("GUARANTEE_OPTION"));
	//			pstmt.setString(191, rs.getString("PREM_ALLOC_YEAR_INDI"));
	//			pstmt.setBigDecimal(192, rs.getBigDecimal("NAR"));
	//			pstmt.setString(193, rs.getString("HIDE_VALIDATE_DATE_INDI"));
	//			pstmt.setBigDecimal(194, rs.getBigDecimal("ETI_SUR_PB"));
	//			pstmt.setBigDecimal(195, rs.getBigDecimal("ETA_PAIDUP_AMOUNT"));
	//			pstmt.setBigDecimal(196, rs.getBigDecimal("ORI_PREMIUM_ADJUST_YEARLY"));
	//			pstmt.setBigDecimal(197, rs.getBigDecimal("ORI_PREMIUM_ADJUST_HALFYEARLY"));
	//			pstmt.setBigDecimal(198, rs.getBigDecimal("ORI_PREMIUM_ADJUST_QUARTERLY"));
	//			pstmt.setBigDecimal(199, rs.getBigDecimal("ORI_PREMIUM_ADJUST_MONTHLY"));
	//			pstmt.setBigDecimal(200, rs.getBigDecimal("CLAIM_DISABILITY_RATE"));
	//			pstmt.setString(201, rs.getString("HEALTH_INSURANCE_INDI"));
	//			pstmt.setString(202, rs.getString("INSURANCE_NOTICE_INDI"));
	//			pstmt.setString(203, rs.getString("SERVICES_BENEFIT_LEVEL"));
	//			pstmt.setString(204, rs.getString("PAYMENT_FREQ"));
	//			pstmt.setString(205, rs.getString("ISSUE_TYPE"));
	//			pstmt.setString(206, rs.getString("LOADING_RATE_POINTER_REASON"));
	//			pstmt.setString(207, rs.getString("RELATION_TO_PH_ROLE"));
	//			pstmt.setBigDecimal(208, rs.getBigDecimal("MONEY_DENOMINATED"));
	//			pstmt.setBigDecimal(209, rs.getBigDecimal("ORIGIN_PRODUCT_VERSION_ID"));
	//			pstmt.setTimestamp(210, ((rs.getDate("TRANSFORM_DATE") == null)? null : new Timestamp(rs.getDate("TRANSFORM_DATE").getTime())));
	//			pstmt.setString(211, rs.getString("HEALTH_INSURANCE_VERSION"));
	//
	//			pstmt.addBatch();
	//
	//			if (count % 3000 == 0) {
	//				pstmt.executeBatch();//executing the batch  
	//				sinkConn.commit(); 
	//				pstmt.clearBatch();
	//			}
	//		}
	//		//	if (startSeq % 50000000 == 0) {
	//		//				
	//		//	cnsl = System.console();
	//		//				logger.info("   >>>roletype={}, startSeq={}, count={}, span={} ", roleType, startSeq, count, (System.currentTimeMillis() - t0));
	//		//	cnsl.printf("   >>>table=%s, startSeq=%d, endSeq=%d, count=%d \n", loadBean.tableName, loadBean.startSeq, loadBean.endSeq, count);
	//
	//		//				cnsl.printf("   >>>roletype=" + roleType + ", startSeq=" + startSeq + ", count=" + count +", span=" + ",span=" + (System.currentTimeMillis() - t0));
	//		//	cnsl.flush();
	//		//		}
	//
	//		pstmt.executeBatch();
	//		if (count > 0) {
	//			sinkConn.commit(); 
	//			logger.info(">>>>> loadContractProductLog count={}, sql={}, startSeq={}, endSeq={}", count, sql, loadBean.startSeq, loadBean.endSeq);
	//		}
	//	}  catch (Exception e) {
	//		logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
	//		map.put("RETURN_CODE", "-999");
	//		map.put("SQL", sql);
	//		map.put("SINK_TABLE", this.sinkTableContractProductLog);
	//		map.put("ERROR_MSG", e.getMessage());
	//		map.put("STACK_TRACE", ExceptionUtils.getStackTrace(e));
	//	} finally {
	//		if (rs != null) {
	//			try {
	//				rs.close();
	//			} catch (SQLException e) {
	//				// TODO Auto-generated catch block
	//				e.printStackTrace();
	//			}
	//		}
	//		if (pstmtSource != null) {
	//			try {
	//				pstmtSource.close();
	//			} catch (SQLException e) {
	//				// TODO Auto-generated catch block
	//				e.printStackTrace();
	//			}
	//		}
	//		if (pstmt != null) {
	//			try {
	//				pstmt.close();
	//			} catch (SQLException e) {
	//				// TODO Auto-generated catch block
	//				e.printStackTrace();
	//			}
	//		}
	//		if (sourceConn != null) {
	//			try {
	//				sourceConn.close();
	//			} catch (SQLException e) {
	//				// TODO Auto-generated catch block
	//				e.printStackTrace();
	//			}
	//		}
	//		if (sinkConn != null) {
	//			try {
	//				sinkConn.close();
	//			} catch (SQLException e) {
	//				// TODO Auto-generated catch block
	//				e.printStackTrace();
	//			}
	//		}
	//	}
	//	return map;
	//}
	//	private void runCreateIndexes() {
	//
	//		long t0 = System.currentTimeMillis();
	//		createIndex("CREATE INDEX IDX_PARTY_CONTACT_1 ON " + this.sinkTablePartyContact + " (MOBILE_TEL) INLINE_SIZE 10 PARALLEL 8");
	//		logger.info(">>>>> create index mobile span={}", (System.currentTimeMillis() - t0));
	//
	//		t0 = System.currentTimeMillis();
	//		createIndex("CREATE INDEX IDX_PARTY_CONTACT_2 ON " + this.sinkTablePartyContact + " (EMAIL)  INLINE_SIZE 20 PARALLEL 8");
	//		logger.info(">>>>> create index email span={}", (System.currentTimeMillis() - t0));
	//
	//		t0 = System.currentTimeMillis();
	//		createIndex("CREATE INDEX IDX_PARTY_CONTACT_3 ON " + this.sinkTablePartyContact + " (ADDRESS_1)  INLINE_SIZE 60 PARALLEL 8");
	//		logger.info(">>>>> create index address span={}", (System.currentTimeMillis() - t0));
	//
	//		t0 = System.currentTimeMillis();
	//		createIndex("CREATE INDEX IDX_STREAMING_ETL_HEALTH_1 ON " + this.sinkTableStreamingEtlHealth + " (CDC_TIME) PARALLEL 8");
	//		logger.info(">>>>> create index streamingetlhealth cdc_time span={}", (System.currentTimeMillis() - t0));
	//
	//		
	//		/*
	//		List<String> indexSqlList = new ArrayList<>();
	//		indexSqlList.add("CREATE INDEX IDX_PARTY_CONTACT_1 ON " + config.sinkTablePartyContact + " (MOBILE_TEL)");
	//		indexSqlList.add("CREATE INDEX IDX_PARTY_CONTACT_2 ON " + config.sinkTablePartyContact + " (EMAIL)");
	//		indexSqlList.add("CREATE INDEX IDX_PARTY_CONTACT_3 ON " + config.sinkTablePartyContact + " (ADDRESS_ID)");
	//		indexSqlList.add("CREATE INDEX IDX_PARTY_CONTACT_TEMP_1 ON " + config.sinkTablePartyContactTemp + " (ADDRESS_ID)");
	//		ExecutorService executor = Executors.newFixedThreadPool(THREADS);
	//
	//		List<CompletableFuture<Integer>> futures = 
	//				indexSqlList.stream().map(t -> CompletableFuture.supplyAsync(
	//						() -> createIndex(t), executor))
	//				.collect(Collectors.toList());			
	//
	//
	//		List<Integer> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
	//		 */
	//	}
}
