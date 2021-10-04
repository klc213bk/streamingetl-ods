package com.transglobe.streamingetl.ods.load.bean;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.transglobe.streamingetl.common.bean.CommonConstants;
import com.transglobe.streamingetl.common.util.OracleUtils;
import com.transglobe.streamingetl.common.util.StreamingEtlUtils;
import com.transglobe.streamingetl.ods.load.Config;

public abstract class DataLoader {
	private static final Logger logger = LoggerFactory.getLogger(DataLoader.class);

	private static final long STEP_SIZE = 1000000;
	private static final long SUB_STEP_SIZE = 10000;
	private static final int RUN_BATCH_SIZE = 10000;	// adjust this number if out of memory
	
	public static final int DEFAULT_THREADS = 15;
	public static final int DEFAULT_BATCH_COMMIT_SIZE = 1000;
	
	protected BasicDataSource sourceConnectionPool;
	protected BasicDataSource sinkConnectionPool;

	protected Config config;

	protected Date dataDate;

	protected int threads;

	protected int batchCommitSize;
	
	private long subStepSize;
	
	private int runBatchSize;
	
	public DataLoader() {}

	public DataLoader(
			Config config
			, Date dataDate) throws Exception {
		this(DEFAULT_THREADS, DEFAULT_BATCH_COMMIT_SIZE, config, dataDate);
	}
	public DataLoader(
			int threads
			, int batchCommitSize
			, Config config
			, Date dataDate) throws Exception {
		this.threads = threads;
		this.batchCommitSize = batchCommitSize;

		this.subStepSize = SUB_STEP_SIZE;
		this.runBatchSize = RUN_BATCH_SIZE;
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
		
		this.dataDate = dataDate;

	}
	public Connection getSourceConnection() throws SQLException {
		return sourceConnectionPool.getConnection();
	}
	public Connection getSinkConnection() throws SQLException {
		return sinkConnectionPool.getConnection();
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
			long startIndex = minId;

			sourceConn=  getSourceConnection();
			pstmt = sourceConn.prepareStatement(countSql);
			ResultSet cntRs = null;
			List<LoadBean> loadBeanList = new ArrayList<>();
			long recordCount = 0L;
			long endIndex = 0L;
			do {
				endIndex = startIndex + stepSize;
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
						loadBean.startSeq = startIndex;
						loadBean.endSeq = endIndex;
						
						loadBeanList.add(loadBean);

						j++;
					} else {

						while (true) {
							LoadBean loadBean = new LoadBean();
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
			} while (endIndex <= maxId);

			for (int k = 0; k < loadBeanList.size(); k++) {
				LoadBean loadBean = loadBeanList.get(k);
				loadBean.seq = (k+1);
				loadBean.tableName = sourceTableName;
				loadBean.loadBeanSize = loadBeanList.size();
				loadBean.startTime = System.currentTimeMillis();
		
			}

			t1 = System.currentTimeMillis();
			logger.info("src table={}, minId={}, maxId={}, loadbean size={}, recordCount={}, beforeLoadSpan={}", sourceTableName, minId, maxId, loadBeanList.size(), recordCount, (t1-t0));

			List<LoadBean> runList = null;
			
			int runStartIndex = 0;
			int runEndIndex = 0;
			while (runEndIndex < loadBeanList.size()) {
				runEndIndex = runStartIndex + runBatchSize;
				if (runEndIndex >= loadBeanList.size()) {
					runEndIndex = loadBeanList.size();
				}  
				logger.info(">>> runStartIndex={}, runEndIndex={}", runStartIndex, runEndIndex);
				runList = loadBeanList.subList(runStartIndex, runEndIndex);
				List<CompletableFuture<LoadBean>> futures = 
						runList.stream().map(t -> CompletableFuture.supplyAsync(() -> 
						{
							try {
								loadData(t);
							} catch (Exception e) {
								// TODO Auto-generated catch block
								logger.info("error message={}, trace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
							}
							return t;
						}))
						.collect(Collectors.toList());
				
				List<LoadBean> result = futures.stream().map(CompletableFuture::join).collect(Collectors.toList());

				runStartIndex = runEndIndex;
			}

		} finally {
			if (executor != null) executor.shutdown();

			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
			if (sourceConn != null) sourceConn.close();
		}

	}
	public long getSourceRecordsCount() throws SQLException {
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
	public long getSinkRecordsCount() throws SQLException {
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
	protected void loadData(LoadBean loadBean) throws Exception {

		transferData(loadBean, sourceConnectionPool, sinkConnectionPool);

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
	}
	public void gatherTableStats(String schema, String tableName) throws SQLException {
		Connection sinkConn = null;
		CallableStatement cstmt = null;
		try {
			sinkConn = this.sinkConnectionPool.getConnection();

			String sql = "begin \n SYS.DBMS_STATS.GATHER_TABLE_STATS(?,?,cascade=>TRUE,degree=>4); \nend;";

			CallableStatement cstamt = sinkConn.prepareCall(sql);
			cstamt.setString(1, schema);	
			cstamt.setString(2, tableName);	
			cstamt.execute();
		} finally {
			if (cstmt != null) cstmt.close();
			if (sinkConn != null) sinkConn.close();
		}

	}
	protected Long getMinId(String minSql) throws SQLException {
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
	protected Long getMaxId(String maxSql) throws SQLException {
		Connection sourceConn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try {
			sourceConn = getSourceConnection();
			pstmt = sourceConn.prepareStatement(maxSql);
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
	abstract protected String getSinkTableName();
	abstract protected String getSinkTableCreateFileName();
	abstract protected String getSinkTableIndexesCreateFileName();
	abstract protected String getSelectMinIdSql();
	abstract protected String getSelectMaxIdSql();
	abstract protected String getCountSql();
	abstract protected String getSelectSql();
	abstract protected String getInsertSql();
	abstract protected void transferData(LoadBean loadBean, 
			BasicDataSource sourceConnectionPool
			, BasicDataSource sinkConnectionPool) throws Exception;
}
