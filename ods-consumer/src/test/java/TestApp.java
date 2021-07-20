

import java.io.Console;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestApp {
	private static final Logger logger = LoggerFactory.getLogger(TestApp.class);

	private static class Config {
		String sourceDbDriver = "oracle.jdbc.driver.OracleDriver";

		String sourceDbUrl = "jdbc:oracle:thin:@10.67.67.63:1521:ebaouat1";

		String sourceDbUsername = "ls_ebao";

		String sourceDbPassword = "ls_ebaopwd";

		String srcTableProductionDetail = "T_PRODUCTION_DETAIL";

		String sourceTableProductionDetail = "TEST_T_PRODUCTION_DETAIL";
	}

	Config config;

	public TestApp() throws Exception {
		config = new Config();
	}

	private void run() throws Exception {
		Connection sourceConn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "";
		Console console = null;

		try {
			Class.forName(config.sourceDbDriver);

			sourceConn = DriverManager.getConnection(config.sourceDbUrl, config.sourceDbUsername, config.sourceDbPassword);

			sourceConn.setAutoCommit(false);

			// truncate table
			truncateTable(config.sourceTableProductionDetail, sourceConn);

			// insert table
			insertProductionDetailTable(sourceConn);
			
			// 

		} catch (Exception e) {
			throw e;
		} finally {
			if (sourceConn != null) {
				try {
					sourceConn.close();
				} catch (SQLException e) {
					throw e;
				}
			}

		}

	}
	private void insertProductionDetailTable(Connection conn) throws Exception {
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		String sql = "";
		try {
			sql = "select DETAIL_ID from " + config.srcTableProductionDetail + " fetch next 10 rows only";
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			List<Long> detailIdList = new ArrayList<>();
			while (rs.next()) {
				detailIdList.add(rs.getLong("DETAIL_ID"));
			}
			rs.close();
			pstmt.close();

			sql = "insert into " + config.sourceTableProductionDetail
					+ " select * from " + config.srcTableProductionDetail
					+ " where detail_id = ?";
			pstmt = conn.prepareStatement(sql);
			for (Long detailId : detailIdList) {

				pstmt.setLong(1, detailId);

				pstmt.executeUpdate();

				conn.commit();

				logger.info("insert record detailId={}", detailId);

				Thread.sleep(1000);
			}
			pstmt.close();
		} catch (Exception e) {
			throw e;
		} finally {
			if (rs != null) rs.close();
			if (pstmt != null) pstmt.close();
		}
	}
	private void truncateTable(String tableName, Connection conn) throws SQLException {

		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate("TRUNCATE TABLE " + tableName);
			stmt.close();
		} catch (java.sql.SQLException e) {
			logger.info(">>>  table:" + tableName + " does not exists!!!");
		} finally {
			if (stmt != null) { 
				stmt.close();
			}
		}
	}
	public static void main(String[] args) {
		String profileActive = System.getProperty("profile.active", "");

		TestApp app;
		try {

			app = new TestApp();

			app.run();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
