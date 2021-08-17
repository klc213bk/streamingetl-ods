package com.transglobe.streamingetl.ods.load.bean;

import java.io.Console;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.transglobe.streamingetl.ods.load.InitLoadApp;

public class TImageLoader {

	private static final Logger logger = LoggerFactory.getLogger(TImageLoader.class);

	
	
	public static final String SELECT_MIN_ID_SQL = 
			"select min(IMAGE_ID) as MIN_ID from " + "SOURCE_TABLE_NAME_IMAGE";
	
	public static final String SELECT_MAX_ID_SQL = 
			"select max(IMAGE_ID) as MAX_ID from " + "SOURCE_TABLE_NAME_IMAGE";
	
	public static final String COUNT_SQL = 
			"select count(*) as CNT from " + "SOURCE_TABLE_NAME_IMAGE" + " a where ? <= a.IMAGE_ID and a.IMAGE_ID < ?";
			
	public static final String SELECT_SQL = "select IMAGE_ID,POLICY_ID,IMAGE_TYPE_ID,SEQ_NUMBER,IMAGE_FORMAT,IMAGE_DATA,SCAN_TIME,EMP_ID,HEAD_ID"
			+ ",FILE_CODE,PROCESS_STATUS,GROUP_POLICY_ID,CASE_ID,CHANGE_ID,IMAGE_LOCATION,IMAGE_FILE_NAME"
			+ ",AUTH_CODE,ORGAN_ID,SUB_FILE_CODE,BUSINESS_ORGAN,IS_PRIORITY,ZIP_DATE,IS_CLEAR,LIST_ID,INSERT_TIME"
			+ ",INSERTED_BY,UPDATE_TIME,UPDATED_BY,INSERT_TIMESTAMP,UPDATE_TIMESTAMP,DEPT_ID,COMPANY_CODE"
			+ ",PERSONAL_CODE,BATCH_DEPT_TYPE,BATCH_DATE,BATCH_AREA,BATCH_DOC_TYPE,BOX_NO,REMARK,SIGNATURE"
			+ ",REAL_WIDTH,SIG_SEQ_NUMBER,SCAN_ORDER,ROWID from " 
			+ "SOURCE_TABLE_NAME_IMAGE"
			+ " a where ? <= a.IMAGE_ID and a.IMAGE_ID < ? for update";

	public static String INSERT_SQL = 
			"insert into " + "SINK_TABLE_NAME_IMAGE "
			+  "(IMAGE_ID,POLICY_ID,IMAGE_TYPE_ID,SEQ_NUMBER,IMAGE_FORMAT,IMAGE_DATA,SCAN_TIME,EMP_ID,HEAD_ID"
			+ ",FILE_CODE,PROCESS_STATUS,GROUP_POLICY_ID,CASE_ID,CHANGE_ID,IMAGE_LOCATION,IMAGE_FILE_NAME"
			+ ",AUTH_CODE,ORGAN_ID,SUB_FILE_CODE,BUSINESS_ORGAN,IS_PRIORITY,ZIP_DATE,IS_CLEAR,LIST_ID,INSERT_TIME"
			+ ",INSERTED_BY,UPDATE_TIME,UPDATED_BY,INSERT_TIMESTAMP,UPDATE_TIMESTAMP,DEPT_ID,COMPANY_CODE"
			+ ",PERSONAL_CODE,BATCH_DEPT_TYPE,BATCH_DATE,BATCH_AREA,BATCH_DOC_TYPE,BOX_NO,REMARK,SIGNATURE"
			+ ",REAL_WIDTH,SIG_SEQ_NUMBER,SCAN_ORDER,DATA_DATE,TBL_UPD_TIME,SRC_ROWID,INSERT_CURRENT_SCN)"
			+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_DATE,?,?)";


	public static LoadBean transferData(LoadBean loadBean, 
			BasicDataSource sourceConnectionPool
			, BasicDataSource sinkConnectionPool
			, BasicDataSource logminerConnectionPool
			, Date dataDate){

		Console cnsl = null;
		long t0 = System.currentTimeMillis();
		try (
				Connection sourceConn = sourceConnectionPool.getConnection();
				Connection sinkConn = sinkConnectionPool.getConnection();
				Connection minerConn = logminerConnectionPool.getConnection();
				final PreparedStatement sourcePstmt = sourceConn.prepareStatement(SELECT_SQL);
				final PreparedStatement sinkPstmt = 
						sinkConn.prepareStatement(INSERT_SQL);
				final PreparedStatement minerPstmt = minerConn.prepareStatement("SELECT CURRENT_SCN FROM v$database")     
			)
		{
			sinkConn.setAutoCommit(false); 
			
			sourcePstmt.setLong(1, loadBean.startSeq);
			sourcePstmt.setLong(2, loadBean.endSeq);

			Date tblUpdTime = new Date(System.currentTimeMillis());
			
			Long currentScn = 0L;
			try (final ResultSet rs =
					minerPstmt.executeQuery())
			{
				while (rs.next())
				{
					currentScn = rs.getLong("CURRENT_SCN");
					break;
				}
			} catch (Exception e) {
				logger.error("error message={},stack trace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e) );
			}
			
			try (final ResultSet rs =
					sourcePstmt.executeQuery())
			{
				Long count = 0L;
				while (rs.next())
				{
					count++;
					
					sinkPstmt.clearParameters();
					
					sinkPstmt.setLong(1, rs.getLong("IMAGE_ID"));
					sinkPstmt.setLong(2, rs.getLong("POLICY_ID"));
					sinkPstmt.setLong(3, rs.getLong("IMAGE_TYPE_ID"));
					sinkPstmt.setLong(4, rs.getLong("SEQ_NUMBER"));
					sinkPstmt.setString(5, rs.getString("IMAGE_FORMAT"));
					sinkPstmt.setString(6, null);
					sinkPstmt.setDate(7, rs.getDate("SCAN_TIME"));
					sinkPstmt.setLong(8, rs.getLong("EMP_ID"));
					sinkPstmt.setLong(9, rs.getLong("HEAD_ID"));
					sinkPstmt.setString(10, rs.getString("FILE_CODE"));
					sinkPstmt.setString(11, rs.getString("PROCESS_STATUS"));
					sinkPstmt.setLong(12, rs.getLong("GROUP_POLICY_ID"));
					sinkPstmt.setLong(13, rs.getLong("CASE_ID"));
					sinkPstmt.setLong(14, rs.getLong("CHANGE_ID"));
					sinkPstmt.setLong(15, rs.getLong("IMAGE_LOCATION"));
					sinkPstmt.setString(16, rs.getString("IMAGE_FILE_NAME"));
					sinkPstmt.setString(17, rs.getString("AUTH_CODE"));
					sinkPstmt.setString(18, rs.getString("ORGAN_ID"));
					sinkPstmt.setString(19, rs.getString("SUB_FILE_CODE"));
					sinkPstmt.setString(20, rs.getString("BUSINESS_ORGAN"));
					sinkPstmt.setString(21, rs.getString("IS_PRIORITY"));
					sinkPstmt.setDate(22, rs.getDate("ZIP_DATE"));
					sinkPstmt.setString(23, rs.getString("IS_CLEAR"));
					sinkPstmt.setLong(24, rs.getLong("LIST_ID"));
					sinkPstmt.setDate(25, rs.getDate("INSERT_TIME"));
					sinkPstmt.setLong(26, rs.getLong("INSERTED_BY"));
					sinkPstmt.setDate(27, rs.getDate("UPDATE_TIME"));
					sinkPstmt.setLong(28, rs.getLong("UPDATED_BY"));
					sinkPstmt.setDate(29, rs.getDate("INSERT_TIMESTAMP"));
					sinkPstmt.setDate(30, rs.getDate("UPDATE_TIMESTAMP"));
					sinkPstmt.setLong(31, rs.getLong("DEPT_ID"));
					sinkPstmt.setString(32, rs.getString("COMPANY_CODE"));
					sinkPstmt.setString(33, rs.getString("PERSONAL_CODE"));
					sinkPstmt.setString(34, rs.getString("BATCH_DEPT_TYPE"));
					sinkPstmt.setString(35, rs.getString("BATCH_DATE"));
					sinkPstmt.setString(36, rs.getString("BATCH_AREA"));
					sinkPstmt.setString(37, rs.getString("BATCH_DOC_TYPE"));
					sinkPstmt.setString(38, rs.getString("BOX_NO"));
					sinkPstmt.setString(39, rs.getString("REMARK"));
					sinkPstmt.setString(40, rs.getString("SIGNATURE"));
					sinkPstmt.setLong(41, rs.getLong("REAL_WIDTH"));
					sinkPstmt.setLong(42, rs.getLong("SIG_SEQ_NUMBER"));
					sinkPstmt.setLong(43, rs.getLong("SCAN_ORDER"));
					sinkPstmt.setDate(44, dataDate);
				//	sinkPstmt.setDate(45, tblUpdTime);
					sinkPstmt.setString(45, rs.getString("ROWID"));
					sinkPstmt.setLong(46, currentScn); 
					
					sinkPstmt.addBatch();

					if (count % 1000 == 0) {
						sinkPstmt.executeBatch();//executing the batch  
						sinkConn.commit(); 
						sinkPstmt.clearBatch();
					}
				}
				sinkPstmt.executeBatch();
				if (count > 0) {
					long span = System.currentTimeMillis() - t0;
					loadBean.span = span;
					loadBean.count = count;
					sinkConn.commit(); 
					cnsl = System.console();
					long expectedCompleteTime = (Long)((loadBean.loadBeanSize * span / 1000)/60000);
					cnsl.printf("   >>>insert into %s count=%d, loadbeanseq=%d, loadBeanSize=%d, startSeq=%d, endSeq=%d, span=%d, expectedCompleteTime(min)=%d\n", 
							"SINK_TABLE_NAME_IMAGE", loadBean.count, loadBean.seq, loadBean.loadBeanSize, loadBean.startSeq, loadBean.endSeq, span, expectedCompleteTime);
					cnsl.flush();
				}
			} catch (Exception e) {
				logger.error("error message={},stack trace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e) );
			}
		} catch (Exception e) {
			logger.error("error message={},stack trace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e) );
		}
		return loadBean;
	} 

}
