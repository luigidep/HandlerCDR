package astelu.qtel.handlerCDR;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

public class AllineaTabelleDatabase {
	final static Logger logger = Logger.getLogger(AllineaTabelleDatabase.class.getName());

	// JDBC driver name and database URL
	static final String JDBC_DRIVER_p = "org.postgresql.Driver";
	static final String DB_URL_p = "jdbc:postgresql://localhost:5432/astelu";

	static final String USER_p = "astelu";
	static final String PASS_p = "astelu";

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
			// conn.setAutoCommit(false);
			System.out.println("Opened database successfully");

			// loadClienti(conn);
			//sincronizzaClientiSqlExpress(conn);
			sincronizzaNumeriNonGeografici(conn);

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
					// "/home/luigi/Documenti/work/astelutilities/nbytel20150831.csv");
					"/home/luigi/Pubblici/clienti_20151126.txt");
			BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
			String s;

			Statement stmt = conn.createStatement();
			int t5 = 0, t4 = 0;
			int tot = 0;
			while ((s = br.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(s, ";");
				tot++;
				String ntel = st.nextToken();
				String pivaOrCf = st.nextToken();
				String codmexal = st.nextToken();
				String secondaria = "";
				String azienda = st.nextToken();

				if (st.hasMoreTokens()) {
					secondaria = azienda;
					azienda = st.nextToken();
				}

				String sql = "INSERT INTO clienti (numtel,piva,codmexal,secondaria, azienda)" + " VALUES ('" + ntel
						+ "','" + pivaOrCf + "','" + codmexal + "','" + secondaria + "','" + azienda + "');";
				logger.info(sql);

				stmt.executeUpdate(sql);
			}
			logger.info(tot + "=" + t4 + "+" + t5);
			stmt.close();
			br.close();
			fstream.close();
			conn.commit();

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
	public static void sincronizzaClientiSqlExpress(Connection conn) {

		try {

			HashMap<String, String> clientiS = utility.getNumeriTelefonoClienti(conn);
			logger.info("size clienti sqlexpress:" + clientiS.size());
			Connection connpostgr = getPostGresConn();
			HashMap<String, String> clientiP = utility.getClientiAll(connpostgr);
			logger.info("size clienti postgres:" + clientiP.size());
			connpostgr.close();
			int i = 0;
			Statement stmt = conn.createStatement();
			for (Map.Entry<String, String> entry : clientiP.entrySet()) {
				String numtel = entry.getKey();

				if (!clientiS.containsKey(numtel)) {

					logger.info(i + "-clienti:" + numtel + "-" + clientiP.get(numtel));
					String rec = clientiP.get(numtel);
					StringTokenizer st = new StringTokenizer(rec, ";");

					String ntel = numtel;
					String pivaOrCf = st.nextToken();
					String codmexal = st.nextToken();
					String secondaria = "";
					String azienda = st.nextToken();

					if (st.hasMoreTokens()) {
						secondaria = azienda;
						azienda = st.nextToken();
					}

					String sql = "INSERT INTO clienti (numtel,piva,codmexal,secondaria, azienda)" + " VALUES ('" + ntel
							+ "','" + pivaOrCf + "','" + codmexal + "','" + secondaria + "','" + azienda + "');";
					logger.info(sql);

					// stmt.executeUpdate(sql);

				}
				// System.out.println(key + " => " + value);
			}

			/*
			 * 
			 * String sql =
			 * "INSERT INTO clienti (numtel,piva,codmexal,secondaria, azienda)"
			 * + " VALUES ('" + ntel + "','" + pivaOrCf + "','" + codmexal +
			 * "','" + secondaria + "','" + azienda + "');"; logger.info(sql);
			 * 
			 * stmt.executeUpdate(sql);
			 */

			stmt.close();
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
	public static void sincronizzaNumeriNonGeografici(Connection conn) {

		try {

			HashMap<String, String> nngS = utility.getNumeriNonGeografici(conn);
			logger.info("nng sqlexpress:" + nngS.size());
			Connection connpostgr = getPostGresConn();
			HashMap<String, String> nngP = utility.getNumeriNonGeograficiAll(connpostgr);
			logger.info("nng postgres:" + nngP.size());
			connpostgr.close();
			int i = 0;
			Statement stmt = conn.createStatement();
			for (Map.Entry<String, String> entry : nngP.entrySet()) {
				String numtel = entry.getKey();
                ++i;
				if (!nngS.containsKey(numtel)) {

					logger.info(i + "-nng:" + numtel + "-" + nngP.get(numtel));
					String rec = nngP.get(numtel);
					StringTokenizer st = new StringTokenizer(rec, ";");
					String descrizione=st.nextToken();
					Double vend_scatto = new Double(st.nextToken());
					Double vend_euro_min = new Double(st.nextToken());
					Double acq_scatto = new Double(st.nextToken());
					Double acq_euro_min = new Double(st.nextToken());

					String sql = "INSERT INTO numerinongeografici (numero, descrizione, [Vendita Scatto], [vendita euro al minuto], [acquisto scatto], [acquisto euro al minuto])"
							+ " VALUES ('"
							+ numtel
							+ "','"
							+ descrizione
							+ "',"
							+ vend_scatto
							+ ","
							+ vend_euro_min
							+ ","
							+ acq_scatto
							+ ","
							+ acq_euro_min
							+ ");";
					logger.info(sql);

					stmt.executeUpdate(sql);

				}
				// System.out.println(key + " => " + value);
			}

			/*
			 * 
			 * String sql =
			 * "INSERT INTO clienti (numtel,piva,codmexal,secondaria, azienda)"
			 * + " VALUES ('" + ntel + "','" + pivaOrCf + "','" + codmexal +
			 * "','" + secondaria + "','" + azienda + "');"; logger.info(sql);
			 * 
			 * stmt.executeUpdate(sql);
			 */

			stmt.close();
			// conn.commit();

		} catch (Exception e) {
			// TODO: handle exception
			logger.error("Exception e:" + e.getMessage());

		}

	}
	

	public static Connection getPostGresConn() {
		// TODO Auto-generated method stub
		Connection conn = null;
		try {

			Class.forName(JDBC_DRIVER_p);
			conn = DriverManager.getConnection(DB_URL_p, USER_p, PASS_p);
			// conn.setAutoCommit(false);
			System.out.println("Opened database successfully");
		} catch (Exception e) {
			System.out.println("Exception e:" + e);
		}
		return conn;
	}
}
