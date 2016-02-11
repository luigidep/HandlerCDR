package astelu.qtel.handlerCDR;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

public class LoadClienti_sqlexpress {
	final static Logger logger = Logger.getLogger(LoadClienti_sqlexpress.class.getName());

	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "net.sourceforge.jtds.jdbc.Driver";
	static final String DB_URL = "jdbc:jtds:sqlserver://server-hp/SQLEXPRESS;databaseName=Mexal_Telefonate";
	
	// Database credentials
	static final String USER = "jdbc";
	static final String PASS = "jdbc";

	public static void main(String[] args) {
		Connection conn = null;
		Statement stmt = null;
		try {

			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			//conn.setAutoCommit(false);
			System.out.println("Opened database successfully");

			loadClienti(conn);

			conn.close();
		} catch (SQLException se) {
			// Handle errors for JDBC
			se.printStackTrace();
		} catch (Exception e) {
			// Handle errors for Class.forName
			e.printStackTrace();
		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se) {
				logger.error("Exception se:"+se.getMessage());
			}// nothing we can do
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se1) {
				logger.error("Exception se1:"+se1.getMessage());
			}// end finally try
		}// end try
		logger.info("Good bye");
	}// end main

	/**
	 * 
	 * @param conn
	 * @param pathdir
	 */
	public static void loadClienti(Connection conn) {

		try {

			FileInputStream fstream = new FileInputStream(
					"/home/luigi/Documenti/work/astelutilities/nbytel20150831.csv");
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fstream));
			String strLine;

			Statement stmt = conn.createStatement();
			while ((strLine = br.readLine()) != null) {
				String sql = "INSERT INTO clienti (numtel, azienda)"
						+ " VALUES ('"
						+ strLine
						+ "','"
						+ "BYT"
						+ "');";

				stmt.executeUpdate(sql);
			}

			stmt.close();
			br.close();
			fstream.close();
			//conn.commit();

		} catch (Exception e) {
			// TODO: handle exception
			logger.error("Exception e:"+e.getMessage());

		}

	}

	
}
