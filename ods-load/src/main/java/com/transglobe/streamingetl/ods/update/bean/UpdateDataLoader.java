package com.transglobe.streamingetl.ods.update.bean;

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
import com.transglobe.streamingetl.ods.load.bean.DataLoader;
import com.transglobe.streamingetl.ods.load.bean.LoadBean;

public abstract class UpdateDataLoader extends DataLoader {
	private static final Logger logger = LoggerFactory.getLogger(UpdateDataLoader.class);

	public UpdateDataLoader() {}

	public UpdateDataLoader(
			Config config
			, String fromUpdateTime
			, String toUpdateTime) throws Exception {
		this(DataLoader.DEFAULT_THREADS, DataLoader.DEFAULT_BATCH_COMMIT_SIZE, config, fromUpdateTime, toUpdateTime);

	}
	public UpdateDataLoader(
			int threads
			, int batchCommitSize
			, Config config
			, String fromUpdateTime
			, String toUpdateTime) throws Exception {
		super(threads, batchCommitSize, config, null);
	

	}
	public void createUpdateTable() throws Exception {
		String updateTableCreateFileName = getUpdateTableCreateFileName();
		Connection sinkConn = null;
		try {
			sinkConn = getUpdateConnection();
			OracleUtils.executeScriptFromFile(updateTableCreateFileName, sinkConn);

		} finally {
			try {
				if (sinkConn != null) sinkConn.close();
			} catch (Exception e) {
				logger.error("message={}, stack trace={}", e.getMessage(), ExceptionUtils.getStackTrace(e));
			}
		}
	}
	private Connection getUpdateConnection() throws SQLException {
		return sinkConnectionPool.getConnection();
	}
	public void dropUpdateTable() throws Exception {
		String updateTableName = getUpdateTableName();
		Connection updateConn = null;
		try {
			updateConn = getUpdateConnection();
			OracleUtils.dropTable(updateTableName, updateConn);

		} finally {
			try {
				if (updateConn != null) updateConn.close();
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
//		PreparedStatement pstmt = null;
		ResultSet rs = null;

		ExecutorService executor = null;

		long t0 = 0L;
		long t1 = 0L;
		try {
			t0 = System.currentTimeMillis();

			executor = Executors.newFixedThreadPool(threads);

			long stepSize = 1000000L;
			long startIndex = minId;

			sourceConn=  getSourceConnection();
//			pstmt = sourceConn.prepareStatement(countSql);
//			ResultSet cntRs = null;
			List<LoadBean> loadBeanList = new ArrayList<>();
			long recordCount = 0L;
			long endIndex = 0L;
			do {
				endIndex = startIndex + stepSize;
//				pstmt.setLong(1, startIndex);
//				pstmt.setLong(2, endIndex);
//				cntRs = pstmt.executeQuery();
//				Integer cnt = 0;
//				if (cntRs.next()) {
//					cnt = cntRs.getInt("CNT");
//				}
//				recordCount += cnt;

				int j = 0;
//				if (cnt > 0) {
//					if (cnt <= batchCommitSize) {
						LoadBean loadBean = new LoadBean();			
						loadBean.startSeq = startIndex;
						loadBean.endSeq = endIndex;
						
						loadBeanList.add(loadBean);

						j++;
//					} else {
//
//						while (true) {
//							LoadBean loadBean = new LoadBean();
//							loadBean.startSeq = startIndex + j * subStepSize;
//							loadBean.endSeq = startIndex + (j + 1) * subStepSize;
//							
//							loadBeanList.add(loadBean);
//
//							j++;
//
//							if (loadBean.endSeq == endIndex) {
//								break;
//							}
//						}
//					}
//				}
				logger.info("count src table={}, startIndex= {}, endIndex={}, loadbeans={}", sourceTableName, startIndex, endIndex, j);

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
			int runBatchSize = 10000;
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
//			if (pstmt != null) pstmt.close();
			if (sourceConn != null) sourceConn.close();
		}

	}
	@Override
	protected String getSinkTableName() {
		return null;
	}
	@Override
	protected String getSinkTableCreateFileName() {
		return null;
	}
	@Override
	protected String getSinkTableIndexesCreateFileName() {
		return null;
	}
	abstract protected String getUpdateTableName();
	abstract protected String getUpdateTableCreateFileName();
}
