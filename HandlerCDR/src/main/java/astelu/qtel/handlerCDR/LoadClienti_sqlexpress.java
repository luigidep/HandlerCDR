package astelu.qtel.handlerCDR;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

public class LoadClienti_sqlexpress {
	final static Logger logger = Logger.getLogger(LoadClienti_sqlexpress.class.getName());

	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "net.sourceforge.jtds.jdbc.Driver";
	static final String DB_URL = "jdbc:jtds:sqlserver://server-hp/SQLEXPRESS;databaseName=Mexal_Telefonate";
	static final String DB_URL_dwpbi = "jdbc:jtds:sqlserver://server-hp/SQLEXPRESS;databaseName=DwPbi";

	// Database credentials
	static final String USER = "jdbc";
	static final String PASS = "jdbc";

	public static void main(String[] args) {
		Connection conn = null;
		Statement stmt = null;
		try {

			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			// conn.setAutoCommit(false);
			System.out.println("Opened database successfully");

			//loadClientiAgg(conn);

			updateClientiServizi(conn);

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
				logger.error("Exception se:" + se.getMessage());
			} // nothing we can do
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se1) {
				logger.error("Exception se1:" + se1.getMessage());
			} // end finally try
		} // end try
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
			BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
			String strLine;

			Statement stmt = conn.createStatement();
			while ((strLine = br.readLine()) != null) {
				String sql = "INSERT INTO clientiservizi (numtel, azienda)" + " VALUES ('" + strLine + "','" + "BYT"
						+ "');";

				stmt.executeUpdate(sql);
			}

			stmt.close();
			br.close();
			fstream.close();
			// conn.commit();

		} catch (Exception e) {
			// TODO: handle exception
			logger.error("Exception e:" + e.getMessage());

		}

	}

	/**
	 * 
	 * @param conn
	 * @param pathdir
	 */
	public static void loadClientiAgg(Connection conn) {

		try {

			FileInputStream fstream = new FileInputStream(
					"/home/luigi/Documenti/work/astelutilities/ArchivioClienti.csv");
			BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
			String strLine;

			Statement stmt = conn.createStatement();
			int i = 0;
			br.readLine();// salta la prima riga
			while ((strLine = br.readLine()) != null) {

				System.out.println((++i) + "-line:" + strLine);
				String[] t = strLine.split(";");
				// if (t.length!=10)
				// logger.warn(strLine+"---length----------------------------------------="+t.length);

				int j = -1;
				String ragsoc = t[++j];
				String telefono = t[++j];
				String cps = t[++j];
				String data_cps = t[++j];
				String adsl = t[++j];
				String tgu = "";
				String wlr = t[++j];
				String voip = t[++j];
				String note = t[++j];
				String agente = t[++j];

				ragsoc = ragsoc.replaceAll("'", "''");
				note = note.replaceAll("'", "''");
				agente = agente.replaceAll("'", "''");

				String sql = "INSERT INTO clientiservizi (ragsoc,telefono,adsl,tgu,cps,wlr,voip,note,agente)"
						+ " VALUES ('" + ragsoc + "','" + telefono + "','" + adsl + "','" + tgu + "','" + cps + "','"
						+ wlr + "','" + voip + "','" + note + "','" + agente + "');";

				stmt.executeUpdate(sql);

			}

			stmt.close();
			br.close();
			fstream.close();
			// conn.commit();

		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception e:" + e.getMessage());

		}

	}

	/**
	 * 
	 * @param conn
	 * @param pathdir
	 */
	public static void updateClientiServizi(Connection conn) {

		Statement st = null;
		ResultSet rs = null;

		try {

			Connection con1 = DriverManager.getConnection(DB_URL_dwpbi, USER, PASS);

			st = conn.createStatement();
			String sql = "SELECT ragsoc from clientiservizi";
			rs = st.executeQuery(sql);
			while (rs.next()) {

				Statement st1;
				st1 = con1.createStatement();
				ResultSet rs1;
				rs1 = st1.executeQuery(
						"SELECT codice,PartitaIVA,CodiceFiscale,Listinodivendita from dimcliente where ragsoc="
								+ rs.getString("ragsoc"));
				if (rs1.next()) {
					String sqlu = "UPDATE clientiservizi set codm=" + rs1.getString("codice") + ", piva="
							+ rs1.getString("PartitaIVA") + ", codfisc=" + rs1.getString("CodiceFiscale") + ", lv="
							+ rs1.getString("Listinodivendita") + " where ragsoc=" + rs.getString("ragsoc");
					st1.executeQuery(sqlu);
				} else {
					logger.warn("cliente non trovato:" + rs.getString("ragsoc"));
				}

			}
			rs.close();
			st.close();

		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception e:" + e.getMessage());

		}

	}

}
