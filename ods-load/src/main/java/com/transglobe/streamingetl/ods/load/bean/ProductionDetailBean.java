package com.transglobe.streamingetl.ods.load.bean;

import java.io.Console;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.transglobe.streamingetl.ods.load.InitialLoadApp;
import com.transglobe.streamingetl.ods.load.InitialLoadApp.LoadBean;

public class ProductionDetailBean {
	private static final Logger logger = LoggerFactory.getLogger(ProductionDetailBean.class);
	
	public static LoadBean loadToSinkTable(LoadBean loadBean, 
			BasicDataSource sourceConnectionPool, BasicDataSource sinkConnectionPool){
		Console cnsl = null;
		Map<String, String> map = new HashMap<>();
		Connection sourceConn = null;
		Connection sinkConn = null;
		PreparedStatement pstmtSource = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = null;
		long t0 = System.currentTimeMillis();
		long t1 = 0L;
		long t2 = 0L;
		try {
			sql = "";
			sourceConn = sourceConnectionPool.getConnection();
			sinkConn = sinkConnectionPool.getConnection();

			pstmtSource = sourceConn.prepareStatement(sql);
			pstmtSource.setLong(1, loadBean.startSeq);
			pstmtSource.setLong(2, loadBean.endSeq);
			rs = pstmtSource.executeQuery();

			t1 = System.currentTimeMillis();
			
			sinkConn.setAutoCommit(false); 

			pstmt = sinkConn.prepareStatement(
					"insert into " + InitialLoadApp.SOURCE_TABLE_NAME_PRODUCTION_DETAIL
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

				if (count % InitialLoadApp.BATCH_COMMIT_SIZE == 0) {
					pstmt.executeBatch();//executing the batch  
					sinkConn.commit(); 
					pstmt.clearBatch();
				}
			}
			t2 = System.currentTimeMillis();

			loadBean.count = count;
			
			pstmt.executeBatch();
			if (count > 0) {
				double avgSpan = ((double)(t2 - t0)) / count;
				loadBean.span = t2 - t0;
				sinkConn.commit(); 
				cnsl = System.console();
				cnsl.printf("   >>>insert into %s count=%d, seq=%d, loadBeanSize=%d, startSeq=%d, endSeq=%d, span=%d, avgSpan=%f, startspan=%d, spantotal=%f \n", 
						InitialLoadApp.SOURCE_TABLE_NAME_PRODUCTION_DETAIL, count, loadBean.seq, loadBean.loadBeanSize, loadBean.startSeq, loadBean.endSeq, (t2 - t0), avgSpan, (t2-loadBean.startTime), (loadBean.loadBeanSize*(t2 - t0)/(double)1000));
				cnsl.flush();
			}


		}  catch (Exception e) {
			map.put("RETURN_CODE", "-999");
			map.put("SQL", sql);
			map.put("SINK_TABLE", InitialLoadApp.SOURCE_TABLE_NAME_PRODUCTION_DETAIL);
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
		return loadBean;
	}
}
