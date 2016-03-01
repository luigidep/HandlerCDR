package astelu.qtel.handlerCDR;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class utility {
	final static Logger logger = Logger.getLogger(utility.class.getName());
	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "org.postgresql.Driver";
	static final String DB_URL = "jdbc:postgresql://localhost:5432/astelu";
	// Database credentials
	static final String USER = "astelu";
	static final String PASS = "astelu";

	static HashMap<String, String> numTelClienti = null;
	static HashMap<String, String> prefissiItaliaMobile = null;
	static HashMap<String, String> prefissiItaliaMobileWLR = null;
	static HashMap<String, String> prefissiItalia = null;
	static HashMap<String, String> prefissiInternazionali = null;
	static HashMap<String, String> prefissiInternazionaliWLR = null;
	static HashMap<String, String> numeriNonGeografici = null;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Connection conn = null;
		Statement stmt = null;
		try {

			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			// conn.setAutoCommit(false);
			System.out.println("Opened database successfully");
			initHashMapTable(conn);
			/*
			 * String prf = getTipoChiamataColt("0438584844","0044775544456");
			 * logger.info("prf:" + prf); prf =
			 * getTipoChiamataColt("0438584844","391775544456");
			 * logger.info("prf:" + prf);
			 * 
			 * prf = getTipoChiamataColt("04364246","0436866301");
			 * logger.info("prf:" + prf);
			 * 
			 * prf = getTipoChiamataTiscali("0438584844","445544456");
			 * logger.info("prf:" + prf);
			 * 
			 * prf = getTipoChiamataTiscali("0438584844","390438584844");
			 * logger.info("prf:" + prf);
			 * 
			 * prf = getTipoChiamataTiscali("0438584844","393918584844");
			 * logger.info("prf:" + prf); //mancano tanti prefissi cellulari
			 * conn.close();
			 */
			// sqlExpress_testConn();

			String prf = getTipoChiamata_WLR_VOIP("0438584844", "44775544456");
			logger.info("prf:" + prf);
			prf = getTipoChiamata_WLR_VOIP("0438584844", "390438544456");
			logger.info("prf:" + prf);
			prf = getTipoChiamata_WLR_VOIP("0438584844", "39190");
			logger.info("prf:" + prf);
			prf = getTipoChiamata_WLR_VOIP("0438584844", "39800800032");
			logger.info("prf:" + prf);

			// } catch (SQLException se) {
			// Handle errors for JDBC
			// se.printStackTrace();
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
	 */
	public static void initHashMapTable(Connection conn) {

		numTelClienti = getNumeriTelefonoClienti(conn);
		prefissiItaliaMobile = getPrefissiItaliaMobile(conn, "prefissi");
		prefissiItaliaMobileWLR = getPrefissiItaliaMobile(conn, "prefissiWlr");
		prefissiItalia = getPrefissiItalia(conn);
		prefissiInternazionali = getPrefissiInternazionali(conn, "prefissi");
		prefissiInternazionaliWLR = getPrefissiInternazionali(conn, "prefissiWlr");
		numeriNonGeografici = getNumeriNonGeografici(conn);

	}

	/**
	 * 
	 * @param conn
	 * @param table
	 * @return
	 */
	public static HashMap<String, String> getPrefissiInternazionali(Connection conn, String table) {
		HashMap<String, String> hm = new HashMap<String, String>();
		Statement st = null;
		ResultSet rs = null;
		try {

			st = conn.createStatement();

			String sql = "SELECT prefisso,paese,cellulare from " + table;

			rs = st.executeQuery(sql);
			String mobile = "mobile";
			String tipoChiamata = "internazionale";
			while (rs.next()) {
				String pr = rs.getString(1);
				if (StringUtils.equals("SI", rs.getString(3)))
					tipoChiamata += " " + mobile;
				tipoChiamata += "_" + rs.getString(2);
				hm.put(pr, tipoChiamata);
				tipoChiamata = "internazionale";
			}
		} catch (Exception e) {
			logger.error("Exception e:" + e.getMessage());

		} finally {
			try {
				if (rs != null)
					rs.close();
				if (st != null)
					st.close();
			} catch (Exception e2) {
				logger.error("Exception e2:" + e2.getMessage());
			}
		}
		return hm;
	}

	/**
	 * 
	 * @param conn
	 * @param table
	 * @return
	 */
	public static HashMap<String, Long> getClientiServizi(Connection conn) {
		HashMap<String, Long> hm = new HashMap<String, Long>();
		Statement st = null;
		ResultSet rs = null;
		try {

			st = conn.createStatement();

			String sql = "SELECT id,ragsoc from ClientiServizi";

			rs = st.executeQuery(sql);

			while (rs.next())
				hm.put(rs.getString(2),rs.getLong(1));

			
		} catch (Exception e) {
			logger.error("Exception e:" + e.getMessage());

		} finally {
			try {
				if (rs != null)
					rs.close();
				if (st != null)
					st.close();
			} catch (Exception e2) {
				logger.error("Exception e2:" + e2.getMessage());
			}
		}
		return hm;
	}
	
	/**
	 * 
	 * @param conn
	 * @param table
	 * @return
	 */
	public static HashMap<String, String> getNumeriNonGeografici(Connection conn) {
		HashMap<String, String> hm = new HashMap<String, String>();
		Statement st = null;
		ResultSet rs = null;
		try {

			st = conn.createStatement();

			String sql = "SELECT numero, descrizione from numerinongeografici";

			rs = st.executeQuery(sql);
			int maxlen = 45;
			while (rs.next()) {
				String numero = rs.getString(1);
				String descrizione = rs.getString(2);
				if (StringUtils.isNotEmpty(descrizione))
					descrizione = "NNG-" + (descrizione.length() > maxlen ? descrizione.substring(0, maxlen) : descrizione);
				else
					descrizione = "NNG";
				hm.put(numero, descrizione);
			}

		} catch (Exception e) {
			logger.error("Exception e:" + e.getMessage());

		} finally {
			try {
				if (rs != null)
					rs.close();
				if (st != null)
					st.close();
			} catch (Exception e2) {
				logger.error("Exception e2:" + e2.getMessage());
			}
		}
		return hm;
	}

	/**
	 * 
	 * @param conn
	 * @param table
	 * @return
	 */
	public static HashMap<String, String> getNumeriNonGeograficiAll(Connection conn) {
		HashMap<String, String> hm = new HashMap<String, String>();
		Statement st = null;
		ResultSet rs = null;
		try {

			st = conn.createStatement();

			String sql = "SELECT numero, descrizione, vendita_scatto, vendita_euro_minuto, acquisto_scatto, acquisto_euro_minuto from numerinongeografici";

			rs = st.executeQuery(sql);
			while (rs.next()) {
                String numero=rs.getString("numero");
                String descrizione=rs.getString("descrizione") == "" ? "NNG":"NNG-"+rs.getString("descrizione");
                String vs=rs.getString("vendita_scatto");
				String vem=rs.getString("vendita_euro_minuto");
				String as=rs.getString("acquisto_scatto");
				String aem=rs.getString("acquisto_euro_minuto");
			
				String rec=descrizione+";"+vs+";"+vem+";"+as+";"+aem;

				hm.put(numero, rec);
			}

		} catch (Exception e) {
			logger.error("Exception e:" + e.getMessage());

		} finally {
			try {
				if (rs != null)
					rs.close();
				if (st != null)
					st.close();
			} catch (Exception e2) {
				logger.error("Exception e2:" + e2.getMessage());
			}
		}
		return hm;
	}
	
	/**
	 * 
	 * @return
	 */
	public static HashMap<String, String> getPrefissiItalia(Connection conn) {
		HashMap<String, String> hm = new HashMap<String, String>();
		Statement st = null;
		ResultSet rs = null;
		try {

			st = conn.createStatement();

			String sql = "SELECT prefisso,distretto from prefissi_italia";

			rs = st.executeQuery(sql);
			while (rs.next())
				hm.put(rs.getString(1), rs.getString(2));

		} catch (Exception e) {
			logger.error("Exception e:" + e.getMessage());

		} finally {
			try {
				if (rs != null)
					rs.close();
				if (st != null)
					st.close();
			} catch (Exception e2) {
				logger.error("Exception e2:" + e2.getMessage());
			}
		}
		return hm;
	}

	/**
	 * 
	 * @return
	 */
	public static HashMap<String, String> getLastCdrFile(Connection conn, String op) {
		HashMap<String, String> hm = new HashMap<String, String>();
		Statement st = null;
		ResultSet rs = null;
		try {

			st = conn.createStatement();

			String sql = "SELECT nomefile, data_ini_ins from cdr_file where operatore ="+op;

			rs = st.executeQuery(sql);
			while (rs.next())
				hm.put(rs.getString(1), rs.getString(2));

		} catch (Exception e) {
			logger.error("Exception e:" + e.getMessage());

		} finally {
			try {
				if (rs != null)
					rs.close();
				if (st != null)
					st.close();
			} catch (Exception e2) {
				logger.error("Exception e2:" + e2.getMessage());
			}
		}
		return hm;
	}
	
	
	/**
	 * 
	 * @return
	 */
	public static HashMap<String, String> getPrefissiItaliaMobile(Connection conn, String table) {
		HashMap<String, String> hm = new HashMap<String, String>();
		Statement st = null;
		ResultSet rs = null;
		try {

			st = conn.createStatement();

			String sql = "SELECT paese, codice from " + table + " where codice <> ''";

			rs = st.executeQuery(sql);
			while (rs.next()) {
				String paese = rs.getString("paese");
				String codice = rs.getString("codice");
				hm.put(codice, paese);
			}
		} catch (Exception e) {
			logger.error("Exception e:" + e.getMessage());

		} finally {
			try {
				if (rs != null)
					rs.close();
				if (st != null)
					st.close();
			} catch (Exception e2) {
				logger.error("Exception e2:" + e2.getMessage());
			}
		}
		return hm;
	}

	/**
	 * 
	 * @return
	 */
	public static HashMap<String, String> getPrefissiItaliaMobileWLR(Connection conn) {
		HashMap<String, String> hm = new HashMap<String, String>();
		Statement st = null;
		ResultSet rs = null;
		try {

			st = conn.createStatement();

			String sql = "SELECT paese, codice from prefissiWlr where codice <> ''";

			rs = st.executeQuery(sql);
			while (rs.next()) {
				String paese = rs.getString("paese");
				String codice = rs.getString("codice");
				hm.put(codice, paese);
			}
		} catch (Exception e) {
			logger.error("Exception e:" + e.getMessage());

		} finally {
			try {
				if (rs != null)
					rs.close();
				if (st != null)
					st.close();
			} catch (Exception e2) {
				logger.error("Exception e2:" + e2.getMessage());
			}
		}
		return hm;
	}

	/**
	 * 
	 * @return
	 */
	public static HashMap<String, String> getNumeriTelefonoClienti(Connection conn) {
		HashMap<String, String> hm = new HashMap<String, String>();
		Statement st = null;
		ResultSet rs = null;
		try {

			st = conn.createStatement();
			String sql = "SELECT distinct numtel from clienti";
			rs = st.executeQuery(sql);
			while (rs.next()) {
				hm.put(rs.getString("numtel"), rs.getString("numtel"));
			}
			rs.close();
			st.close();

		} catch (Exception e) {
			logger.error("Exception e:" + e.getMessage());

		} finally {
			try {
				if (rs != null)
					rs.close();
				if (st != null)
					st.close();
			} catch (Exception e2) {
				logger.error("Exception e2:" + e2.getMessage());
			}
		}
		return hm;
	}
	/**
	 * 
	 * @return
	 */
	public static HashMap<String, String> getClientiAll(Connection conn) {
		HashMap<String, String> hm = new HashMap<String, String>();
		Statement st = null;
		ResultSet rs = null;
		try {
			st = conn.createStatement();
			String sql = "SELECT numtel,piva,codmexal,secondaria,azienda from clienti";
			rs = st.executeQuery(sql);
			while (rs.next()) {
                String piva=rs.getString("piva") == "" ? "_":rs.getString("piva");
                String codmexal=rs.getString("codmexal") == "" ? "_":rs.getString("codmexal");
                String secondaria=rs.getString("secondaria") == "" ? "_":rs.getString("secondaria");
				String rec=piva+";"+codmexal+";"+secondaria+";"+rs.getString("azienda");
				hm.put(rs.getString("numtel"), rec);
			}
			rs.close();
			st.close();

		} catch (Exception e) {
			logger.error("Exception e:" + e.getMessage());

		} finally {
			try {
				if (rs != null)
					rs.close();
				if (st != null)
					st.close();
			} catch (Exception e2) {
				logger.error("Exception e2:" + e2.getMessage());
			}
		}
		return hm;
	}
	
	
	
	/**
	 * 
	 * @param conn
	 * @param nf
	 * @return
	 * @throws SQLException
	 */
	public static Long insertCdrfile(Connection conn, String nf) throws SQLException {

		Long idfile = null;
		try {
			Timestamp now = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
			/*
			 * INSERT INTO [dbo].[cdr_file] ([nomefile] ,[num_cdr] ,[operatore]
			 * ,[data_ini_ins] ,[data_end_ins]) VALUES (<nomefile,
			 * nvarchar(50),> ,<num_cdr, int,> ,<operatore, nvarchar(30),>
			 * ,<data_ini_ins, datetime2(0),> ,<data_end_ins, datetime2(0),>)
			 */
			String SQL_INSERT = "INSERT INTO cdr_file (nomefile, num_cdr,operatore, data_ini_ins, data_end_ins)"
					+ " values (?,?,?,?)";
			/*
			 * + " VALUES('" + nomefile + "',0,'" + now + "','" + now + "');";
			 */

			PreparedStatement st = conn.prepareStatement(SQL_INSERT, Statement.RETURN_GENERATED_KEYS);
			st.setString(1, nf);
			st.setInt(2, 0);
			st.setTimestamp(3, now);
			st.setTimestamp(4, now);

			st.executeUpdate();

			ResultSet generatedKeys = st.getGeneratedKeys();

			if (generatedKeys.next()) {
				// System.out.println("id is:" + generatedKeys.getLong(1));
				idfile = generatedKeys.getLong(1);
			} else {
				throw new SQLException("Creating user failed, no generated key obtained.");
			}

			st.close();
			generatedKeys.close();

		} catch (Exception e) {
			logger.error("Excetion e:" + e.getMessage());

		}

		return idfile;

	}

	/**
	 * 
	 * @param conn
	 * @param nf
	 * @return
	 * @throws SQLException
	 */
	public static Long insertCdrFile(Connection conn, String nf, String tipoOp) throws SQLException {

		Long idfile = null;
		try {
			Timestamp now = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());

			String SQL_INSERT = "INSERT INTO cdr_file (nomefile, num_cdr,operatore, data_ini_ins, data_end_ins)"
					+ " values (?,?,?,?,?)";

			PreparedStatement st = conn.prepareStatement(SQL_INSERT, Statement.RETURN_GENERATED_KEYS);
			st.setString(1, nf);
			st.setInt(2, 0);
			st.setString(3, tipoOp);
			st.setTimestamp(4, now);
			st.setTimestamp(5, null);

			st.executeUpdate();

			ResultSet generatedKeys = st.getGeneratedKeys();

			if (generatedKeys.next()) {
				idfile = generatedKeys.getLong(1);
			} else {
				throw new SQLException("Creating user failed, no generated key obtained.");
			}

			st.close();
			generatedKeys.close();

		} catch (Exception e) {
			logger.error("Exception e:" + e.getMessage());
		}

		return idfile;

	}

	/**
	 * 
	 * @param conn
	 * @param nf
	 * @return
	 * @throws SQLException
	 */
	public static boolean isLoaded(Connection conn, String nf) throws SQLException {

		boolean isLoaded = false;
		Statement st = conn.createStatement();

		String sql = "SELECT nomefile from cdr_file where nomefile='" + nf + "'";

		ResultSet rs = st.executeQuery(sql);
		while (rs.next()) {
			logger.info("file: " + nf + " già caricato");
			isLoaded = true;
		}
		rs.close();
		st.close();
		return isLoaded;

	}

	/**
	 * 
	 * @param prefissi_italia
	 * @param cte
	 * @param cto
	 * @return
	 */
	private static String getTipoChiamata(String cte, String cto) {

		String mi = checkPrefissiItaliaMobile(cte, cto);
		if (mi != null)
			return mi;

		return checkPrefissiItalia(cte, cto);

	}

	/**
	 * 
	 * @param prefissi_italia
	 * @param cte
	 * @param cto
	 * @return
	 */
	private static String getTipoChiamata(String cte, String cto, HashMap<String, String> pim) {

		String pr = checkPrefissiItaliaMobile(cte, cto, pim);
		if (pr != null)
			return pr;

		pr = checkNumeriNonGeografici(cte, cto);
		if (pr != null)
			return pr;

		return checkPrefissiItalia(cte, cto);

	}

	/**
	 * 
	 * @param prefissi_italia
	 * @param cte
	 * @param cto
	 * @return
	 */
	public static String getTipoChiamataColt(String cte, String cto) {

		if (StringUtils.startsWith(cto, "800"))
			return "verde";

		if (StringUtils.startsWith(cto, "00"))
			return checkPrefissiInternazionali(cto.substring(2));
		else
			return getTipoChiamata(cte, cto);

	}

	/**
	 * 
	 * @param prefissi_italia
	 * @param cte
	 * @param cto
	 * @return
	 */
	public static String getTipoChiamataTiscali(String cte, String cto) {

		if (StringUtils.startsWith(cto, "800"))
			return "verde";

		if (!StringUtils.startsWith(cto, "39"))
			return checkPrefissiInternazionali(cto);
		else
			return getTipoChiamata(cte, cto.substring(2));

	}

	
	/**
	 * 
	 * @param prefissi_italia
	 * @param cte
	 * @param cto
	 * @return
	 */
	public static String getTipoChiamata_WLR_VOIP(String cte, String cto) {
        
		if (StringUtils.startsWith(cto, "800") || StringUtils.startsWith(cte, "800"))
			return "NNG-verde";

		else if (!StringUtils.startsWith(cto, "39"))
			return checkPrefissiInternazionali(cto, prefissiInternazionaliWLR);
		else
			return getTipoChiamata(cte, cto.substring(2), prefissiItaliaMobileWLR);

	}
	
	
	
	/**
	 * 
	 * @param prefissi
	 * @param cte
	 * @param cto
	 * @return
	 */
	private static String checkPrefissiItaliaMobile(String cte, String cto) {

		String mobile = "mobile";
		String prf = StringUtils.substring(cto, 0, 4);// tiscali mobile ha un
														// prefisso di 4 cifre
		if (prefissiItaliaMobile.containsKey(prf))
			return mobile + "_" + prefissiItaliaMobile.get(prf) + "";

		prf = StringUtils.substring(cto, 0, 3);
		if (prefissiItaliaMobile.containsKey(prf))
			return mobile + "_" + prefissiItaliaMobile.get(prf) + "";

		return null;

	}

	/**
	 * 
	 * @param prefissi
	 * @param cte
	 * @param cto
	 * @return
	 */
	private static String checkPrefissiItaliaMobile(String cte, String cto, HashMap<String, String> pim) {

		String mobile = "mobile";
		String prf = StringUtils.substring(cto, 0, 4);// tiscali mobile ha un
														// prefisso di 4 cifre
		if (pim.containsKey(prf))
			return mobile + "_" + pim.get(prf) + "";

		prf = StringUtils.substring(cto, 0, 3);
		if (pim.containsKey(prf))
			return mobile + "_" + pim.get(prf) + "";

		return null;

	}

	/**
	 * 
	 * @param prefissi
	 * @param cte
	 * @param cto
	 * @return
	 */
	private static String checkPrefissiItalia(String cte, String cto) {
		int lenCto = cto.length();
		if (lenCto > 4)
			lenCto = 4;
		String prf = cto.substring(0, lenCto);
		String prfFound = null;
		String distretto = null;
		while (prf.length() > 1) {
			if (prefissiItalia.get(prf) != null) {
				prfFound = prf;
				distretto = prefissiItalia.get(prf);
				break;
			}
			prf = cto.substring(0, prf.length() - 1);

		}

		if (prfFound != null)
			return getTipoChimataItalia(prfFound, cte) + "_" + distretto;
		else
			return "UNKNOW_UNKNOW";

	}

	/**
	 * 
	 * @param prefissi
	 * @param cte
	 * @param cto
	 * @return
	 */
	private static String checkNumeriNonGeografici(String cte, String cto) {

		String prf = cto;
		String descrizione = null;

		while (prf.length() > 1) {
			descrizione = numeriNonGeografici.get(prf);
			if (StringUtils.isNotEmpty(descrizione))
				break;

			prf = cto.substring(0, prf.length() - 1);
		}

		return descrizione;

	}

	/**
	 * 
	 * @param cto
	 * @return
	 */
	public static String checkPrefissiInternazionali(String cto) {
		int lenCto = cto.length();
		if (lenCto > 8)
			lenCto = 8;
		String prf = cto.substring(0, lenCto);
		String prfFound = "UNKNOW_UNKNOW";
		while (prf.length() > 0) {

			if (prefissiInternazionali.get(prf) != null) {
				prfFound = prefissiInternazionali.get(prf);
				break;
			}
			prf = cto.substring(0, prf.length() - 1);

		}

		return prfFound;
	}

	/**
	 * 
	 * @param cto
	 * @return
	 */
	public static String checkPrefissiInternazionali(String cto, HashMap<String, String> pi) {
		int lenCto = cto.length();
		if (lenCto > 8)
			lenCto = 8;
		String prf = cto.substring(0, lenCto);
		String prfFound = "UNKNOW_UNKNOW";
		while (prf.length() > 0) {

			if (pi.get(prf) != null) {
				prfFound = pi.get(prf);
				break;
			}
			prf = cto.substring(0, prf.length() - 1);

		}

		return prfFound;
	}

	/**
	 * 
	 * @param prfound
	 * @param cte
	 * @return
	 */
	private static String getTipoChimataItalia(String prfound, String cte) {
		if (cte.startsWith(prfound))
			return "locale";
		else
			return "nazionale";

	}

	static public void sqlExpress_testConn() {
		String JDBC_DRIVER = "net.sourceforge.jtds.jdbc.Driver";
		// static final String DB_URL =
		// "jdbc:jtds:sqlserver://192.168.125.10:1433/master;instance=SQLEXPRESS";
		String DB_URL = "jdbc:jtds:sqlserver://server-hp/SQLEXPRESS;databaseName=Mexal_Telefonate";

		// jdbc:jtds:sqlserver://server-name/database_name;instance=instance_name

		// Database credentials
		String USER = "jdbc";
		String PASS = "jdbc";

		Connection conn = null;
		Statement stmt = null;
		@SuppressWarnings("unused")
		String numTelSconosciuti = "";
		try {

			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			// conn.setAutoCommit(false);
			logger.info("Opened database successfully, start loading CDR");

			initHashMapTable(conn);
			numTelSconosciuti = loadCdrColt(conn, "/opt/colt/test/");
			// numTelSconosciuti = loadCdrTiscali(conn, LOCALPATH_CDR_TISCALI);

			// Long idfile = insertCdrFile(conn, "prova.cdr");
		} catch (SQLException se) {
			// Handle errors for JDBC
			logger.error("SQLException:" + se.getMessage());
			se.printStackTrace();
		} catch (Exception e) {
			// Handle errors for Class.forName
			logger.error("Exception:" + e.getMessage());
			e.printStackTrace();
		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
				logger.error("closing stmt SQLException :" + se2.getMessage());
			} // nothing we can do
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				logger.error("closing conn SQLException :" + se.getMessage());
			} // end finally try
		} // end try
		logger.info("End loading CDR");

	}// end main

	public static String loadCdrColt(Connection conn, String pathdir) {
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		// MexalAdapter ma= new MexalAdapter();
		TreeMap<String, String> t = FileSystemUtility.getFileList(pathdir, ".cdr");
		Set<Entry<String, String>> set = t.entrySet();
		Iterator<Entry<String, String>> iterator = set.iterator();

		FileInputStream fstream = null;
		BufferedReader br = null;
		Statement stmt = null;
		String esitoCaricamento = null;
		String cdrErrati = "";
		String numTelSconosciuti = "Cdr colt, numeri di telefono non riconosciuti:\n";

		TreeMap<String, String> numTelScon = new TreeMap<String, String>();
		// String prefissiUnknow="";
		String cdr = null;

		try {

			while (iterator.hasNext()) {
				int crday = 0;
				long inifile = System.currentTimeMillis();

				Map.Entry<String, String> me = (Map.Entry<String, String>) iterator.next();
				String nomefile = me.getValue().toString();

				fstream = new FileInputStream(pathdir + nomefile);

				br = new BufferedReader(new InputStreamReader(fstream));

				stmt = conn.createStatement();

				while ((cdr = br.readLine()) != null) {
					StringTokenizer st = new StringTokenizer(cdr);

					if (st.countTokens() != 4) {
						cdrErrati += "num tokens errato:" + st.countTokens() + ", file:" + nomefile + ", cdr:" + cdr
								+ "\n";
						logger.warn(cdrErrati);
						continue;
					}

					++crday;

					@SuppressWarnings("unused")
					String iduser = st.nextToken();
					String cte = st.nextToken();

					if (!utility.numTelClienti.containsKey(cte))
						numTelScon.put(cte, cte);

					String cto = st.nextToken();
					String dateAndDecimi = st.nextToken();
					String time = dateAndDecimi.substring(0, 14);
					Integer decimi = new Integer(dateAndDecimi.substring(16));
					String tipoCh_paese = utility.getTipoChiamataColt(cte, cto);
					String tc = StringUtils.substringBefore(tipoCh_paese, "_");
					String paese = StringUtils.substringAfter(tipoCh_paese, "_");
					paese = paese.replaceAll("'", "''");
					long tm = dateFormat.parse(time).getTime();
					Timestamp ts = new Timestamp(tm);
					// Integer secround = Math.round(new Integer(decimi) / 10f);
					Integer secsup = (int) Math.ceil(new Integer(decimi) / 10f);
					Integer secp1s = secsup + 1;
					String sql = "INSERT INTO dbo.chiamateColt_test (idfile,telcli,teldestinazione,tipoChiamata,dataora,durata_effettiva,durata,durata_p1s,paese)"
							+ " VALUES (" + 1 + ",'" + cte + "','" + cto + "','" + tc + "','" + ts + "'," + decimi + ","
							+ secsup + "," + secp1s + ",'" + paese + "');";
					// logger.info(sql);
					stmt.executeUpdate(sql);

				}

				// conn.commit();

				logger.info(1 + "-" + nomefile + ":" + crday + ": " + ((System.currentTimeMillis() - inifile) / 1000)
						+ " sec.");

				stmt.close();
				br.close();
				fstream.close();
			}
		} catch (Exception e) {
			logger.error("Exception e:" + cdr + ":" + e.getMessage());
			esitoCaricamento = "\nException 2:" + e.getMessage();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
				if (br != null)
					br.close();
				if (fstream != null)
					fstream.close();
				int i = 0;
				for (String nts : numTelScon.keySet()) {
					++i;
					logger.info("numero di telefono non riconosciuto:" + i + "-" + nts);
					numTelSconosciuti += (i) + "-" + nts + ", file:" + numTelScon.get(nts) + "\n";
				}

			} catch (Exception e2) {
				logger.error("Exception e2:" + e2.getMessage());
				esitoCaricamento = "\nException e2:" + e2.getMessage();
			}
		}
		if (esitoCaricamento == null)
			esitoCaricamento = "File CDR caricati correttamente in SQL express.\n";
		else
			esitoCaricamento = "Errore caricamento file CDR in SQL express:" + esitoCaricamento + "\n";

		if (!"".equals(cdrErrati))
			cdrErrati = "\nCDR errati:\n" + cdrErrati;

		return esitoCaricamento + cdrErrati + numTelSconosciuti;
	}

}
