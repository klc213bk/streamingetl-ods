package com.transglobe.streamingetl.ods.load.bean;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.transglobe.streamingetl.common.util.OracleUtils;
import com.transglobe.streamingetl.common.util.StreamingEtlUtils;

public abstract class DataLoader {
	private static final Logger logger = LoggerFactory.getLogger(DataLoader.class);

	private static final String SINK_TABLE_SUPPL_LOG_SYNC = "SUPPL_LOG_SYNC";
	
	private static final long STEP_SIZE = 1000000;
	private static final long SUB_STEP_SIZE = 10000;
	
	protected BasicDataSource sourceConnectionPool;
	protected BasicDataSource sinkConnectionPool;
	protected BasicDataSource logminerConnectionPool;

	protected Config config;

	protected Date dataDate;

	protected int threads;

	protected int batchCommitSize;
	
	public DataLoader() {}
	
	public DataLoader(int threads
			, int batchCommitSize
			, Config config
			, Date dataDate) throws Exception {
		this.threads = threads;
		this.batchCommitSize = batchCommitSize;

		this.config = config;

		sourceConnectionPool = new BasicDataSource();
		sourceConnectionPool.setUrl(config.sourceDbUrl);
		sourceConnectionPool.setUsername(config.sourceDbUsername);
		sourceConnectionPool.setPassword(config.sourceDbPassword);
		sourceConnectionPool.setDriverClassName(config.sourceDbDriver);
		sourceConnectionPool.setMaxTotal(threads);

		sinkConnectionPool = new BasicDataSource();
		sinkConnectionPool.setUrl(config.sinkDbUrl);
		sinkConnectionPool.setUsername(config.sinkDbUsername);
		sinkConnectionPool.setPassword(config.sinkDbPassword);
		sinkConnectionPool.setDriverClassName(config.sinkDbDriver);
		sinkConnectionPool.setMaxTotal(threads);

		logminerConnectionPool = new BasicDataSource();
		logminerConnectionPool.setUrl(config.logminerDbUrl);
		logminerConnectionPool.setUsername(config.logminerDbUsername);
		logminerConnectionPool.setPassword(config.logminerDbPassword);
		logminerConnectionPool.setDriverClassName(config.logminerDbDriver);
		logminerConnectionPool.setMaxTotal(threads);

		this.dataDate = dataDate;

	}
	public Connection getSourceConnection() throws SQLException {
		return sourceConnectionPool.getConnection();
	}
	public Connection getSinkConnection() throws SQLException {
		return sinkConnectionPool.getConnection();
	}

	public Connection getLogminerConnection() throws SQLException {
		return logminerConnectionPool.getConnection();
	}
	public void run() throws Exception {
		logger.info(">>>  Start to STREAMING_ETL loading start");
		startStreamingEtlLoading();
		
		logger.info(">>>  Start to dropTable");
		dropSinkTable();
		
		logger.info(">>>  Start to create sink Table");
		createSinkTable();
		
		logger.info(">>>  Start to load data");
		Long t0 = System.currentTimeMillis();
		loadData();
		logger.info(">>>tableName={}, loaddataSpan={}", getSourceTableName(), (System.currentTimeMillis() - t0));

		
		logger.info(">>>  Start: check source and sink data count");
		long sourceCnt = getSourceRecordsCount();
		long sinkCnt = getSinkRecordsCount();
		if (sourceCnt == sinkCnt) {
			logger.info(">>>  sourceCnt={} equals to sinkCnt={}", sourceCnt, sinkCnt);
		} else {
			throw new Exception("sourceCnt("+ sourceCnt + ") does not equal to sinkCnt(" + sinkCnt + ")");
		}
		
		logger.info(">>>  Start: createIndex");	
		createSinkTableIndex();
		
		logger.info(">>>  Start: init SUPPL_LOG_SYNC");
		initSupplLogSync();
		
		logger.info(">>>  Start to update STREAMING_ETL loading finish");
		updateStreamingEtlLoadingFinish();
	}
	public void dropSinkTable() throws Exception {
		String sinkTableName = getSinkTableName();
		Connection sinkConn = null;
		try {
			sinkConn = getSinkConnection();
			OracleUtils.dropTable(sinkTableName, sinkConn);

		} finally {
			try {
				if (sinkConn != null) sinkConn.close();
			} catch (Exception e) {
				logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}
		}
	}

	public void createSinkTable() throws Exception {
		String sinkTableCreateFileName = getSinkTableCreateFileName();
		Connection sinkConn = null;
		try {
			sinkConn = getSinkConnection();
			OracleUtils.executeScriptFromFile(sinkTableCreateFileName, sinkConn);

		} finally {
			try {
				if (sinkConn != null) sinkConn.close();
			} catch (Exception e) {
				logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}
		}
	}
	public void createSinkTableIndex() throws Exception {
		String sinkTableIndexCreateFileName = getSinkTableIndexesCreateFileName();
		Connection sinkConn = null;
		try {
			sinkConn = getSinkConnection();
			OracleUtils.executeScriptFromFile(sinkTableIndexCreateFileName, sinkConn);

		} finally {
			try {
				if (sinkConn != null) sinkConn.close();
			} catch (Exception e) {
				logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}
		}
	}

	public void startStreamingEtlLoading() throws Exception {
		Connection logminerConn = null;
		try {
			logminerConn = getLogminerConnection();
			StreamingEtlUtils.startStreamingEtlLoading(logminerConn, getStreamingEtlName());
		} finally {
			if (logminerConn != null) logminerConn.close();
		}
	}
	public void updateStreamingEtlLoadingFinish() throws Exception {
		Connection logminerConn = null;
		try {
			logminerConn = getLogminerConnection();
			StreamingEtlUtils.updateStreamingEtlLoadingFinish(logminerConn, getStreamingEtlName());
		} finally {
			if (logminerConn != null) logminerConn.close();
		}
	}
	
	public void initSupplLogSync() throws Exception {
		Connection sinkConn = null;
		Connection logminerConn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "";
		try {

			sinkConn = getSinkConnection();
			logminerConn = getLogminerConnection();

			long t = System.currentTimeMillis();
			sql = "truncate table " + SINK_TABLE_SUPPL_LOG_SYNC;
			pstmt = sinkConn.prepareStatement(sql);
			pstmt.executeUpdate();
			pstmt.close();
			
//			sql = "insert into " + SINK_TABLE_SUPPL_LOG_SYNC 
//					+ " (RS_ID, SSN, SCN, SCN_TIMESTAMP, TABLE_NAME, INSERT_TIME) "
//					+ " values (?,?,?,?,?,?)";
//
//			pstmt = sinkConn.prepareStatement(sql);
//			pstmt.setString(1, "RS-ID");
//			pstmt.setLong(2, 0L);
//			pstmt.setLong(3, 0L);
//			pstmt.setNull(4, Types.TIMESTAMP);
//			pstmt.setString(5, getSourceTableName());
//			pstmt.setLong(6, t);
//
//			pstmt.executeUpdate();

		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (sinkConn != null) sinkConn.close();
			if (logminerConn != null) logminerConn.close();
		}
	}
	public void loadData() throws Exception {
		String selectMinIdSql = getSelectMinIdSql();
		String selectMaxIdSql = getSelectMaxIdSql();
		String countSql = getCountSql();
		String sourceTableName = getSourceTableName();
		Long minId = getMinId(selectMinIdSql);
		Long maxId = getMaxId(selectMaxIdSql);

		Connection sourceConn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		ExecutorService executor = null;

		long t0 = 0L;
		long t1 = 0L;
		try {
			t0 = System.currentTimeMillis();

			executor = Executors.newFixedThreadPool(threads);

			long stepSize = STEP_SIZE;
			long subStepSize = SUB_STEP_SIZE;
			long startIndex = minId;

			sourceConn=  getSourceConnection();
			pstmt = sourceConn.prepareStatement(countSql);
			ResultSet cntRs = null;
			List<LoadBean> loadBeanList = new ArrayList<>();
			long recordCount = 0L;
			while (startIndex <= maxId) {
				long endIndex = startIndex + stepSize;
				pstmt.setLong(1, startIndex);
				pstmt.setLong(2, endIndex);
				cntRs = pstmt.executeQuery();
				Integer cnt = 0;
				if (cntRs.next()) {
					cnt = cntRs.getInt("CNT");
				}
				recordCount += cnt;
				int j = 0;
				if (cnt > 0) {
					if (cnt <= batchCommitSize) {
						LoadBean loadBean = new LoadBean();
						loadBean.tableName = sourceTableName;
						loadBean.startSeq = startIndex;
						loadBean.endSeq = endIndex;

						loadBeanList.add(loadBean);

						j++;
					} else {

						while (true) {
							LoadBean loadBean = new LoadBean();
							loadBean.tableName = sourceTableName;
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
				logger.info("count src table={}, startIndex= {}, endIndex={}, cnt={}, loadbeans={}", sourceTableName, startIndex, endIndex, cnt, j);

				startIndex = endIndex;
			}

			for (int k = 0; k < loadBeanList.size(); k++) {
				LoadBean loadBean = loadBeanList.get(k);
				loadBean.seq = (k+1);
				loadBean.loadBeanSize = loadBeanList.size();
				loadBean.startTime = System.currentTimeMillis();
			}
			
			t1 = System.currentTimeMillis();
			logger.info("src table={}, minId={}, maxId={}, loadbean size={}, recordCount={}, beforeLoadSpan={}", sourceTableName, minId, maxId, loadBeanList.size(), recordCount, (t1-t0));

			List<CompletableFuture<LoadBean>> futures = 
					loadBeanList.stream().map(t -> CompletableFuture.supplyAsync(() -> 
					{
						LoadBean loadBean = null;
						try {
							loadBean = loadData(t);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							logger.info("error message={}, trace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
						}
						return loadBean;
					}))
					.collect(Collectors.toList());


			List<LoadBean> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());

			//			double totalSpan = 0;
			//			long totalCount = 0L;
			//			for (LoadBean loadBean : result) {
			//				totalSpan += loadBean.span; 
			//				totalCount += loadBean.count;
			//			}
			//			long t2 = System.currentTimeMillis();
			//			logger.info(">>>tableName={}, loadBeanSize={}, totalCount={}, loaddataSpan={}", sourceTableName, result.size(), totalCount, (t2 - t1));



		} finally {
			if (executor != null) executor.shutdown();

			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (sourceConn != null) sourceConn.close();
		}

	}
	protected long getSourceRecordsCount() throws SQLException {
		String sourceTableName = getSourceTableName();

		Connection sourceConn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql= "";
		try {
			sourceConn = this.sourceConnectionPool.getConnection();

			sql = "SELECT COUNT(*) CNT from " + sourceTableName;
			pstmt = sourceConn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			long sourceCnt = 0;
			while (rs.next()) {
				sourceCnt = rs.getLong("CNT");
			}
			rs.close();
			pstmt.close();

			return sourceCnt;

		} finally {

			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (sourceConn != null) sourceConn.close();
		}
	}
	protected long getSinkRecordsCount() throws SQLException {
		String sinkTableName = getSinkTableName();

		Connection sinkConn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql= "";
		try {
			sinkConn = this.sinkConnectionPool.getConnection();

			sql = "SELECT COUNT(*) CNT from " + sinkTableName;
			pstmt = sinkConn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			long sinkCnt = 0;
			while (rs.next()) {
				sinkCnt = rs.getLong("CNT");
			}
			rs.close();
			pstmt.close();

			return sinkCnt;

		} finally {

			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (sinkConn != null) sinkConn.close();
		}
	}
	private LoadBean loadData(LoadBean loadBean) throws Exception {

		transferData(loadBean, sourceConnectionPool, sinkConnectionPool, logminerConnectionPool);

		return loadBean;
	}
	public void close() {
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
	private Long getMinId(String minSql) throws SQLException {
		Connection sourceConn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			sourceConn = getSourceConnection();
			pstmt = sourceConn.prepareStatement(minSql);
			rs = pstmt.executeQuery();
			Long minId = null;
			while (rs.next()) {
				minId = rs.getLong("MIN_ID");
			}
			return minId;
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (sourceConn != null) sourceConn.close();
		}

	}
	private Long getMaxId(String minSql) throws SQLException {
		Connection sourceConn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			sourceConn = getSourceConnection();
			pstmt = sourceConn.prepareStatement(minSql);
			rs = pstmt.executeQuery();
			Long maxId = null;
			while (rs.next()) {
				maxId = rs.getLong("MAX_ID");
			}
			return maxId;
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (sourceConn != null) sourceConn.close();
		}

	}

	public abstract String getSourceTableName();
	public abstract String getStreamingEtlName();
	abstract protected String getSinkTableName();
	abstract protected String getSinkTableCreateFileName();
	abstract protected String getSinkTableIndexesCreateFileName();
	abstract protected String getSelectMinIdSql();
	abstract protected String getSelectMaxIdSql();
	abstract protected String getCountSql();
	abstract protected String getSelectSql();
	abstract protected String getInsertSql();
	abstract protected LoadBean transferData(LoadBean loadBean, 
			BasicDataSource sourceConnectionPool
			, BasicDataSource sinkConnectionPool
			, BasicDataSource logminerConnectionPool) throws Exception;
}
