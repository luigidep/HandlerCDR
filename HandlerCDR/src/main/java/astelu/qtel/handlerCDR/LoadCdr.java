package astelu.qtel.handlerCDR;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class LoadCdr {
	final static Logger logger = Logger.getLogger(LoadCdr.class.getName());

	// JDBC driver name and database URL
	static String JDBC_DRIVER = null;
	static String DB_URL = null;
	static int NUMTOKEN_CDR = 0;
	static String sep = "\t";
	static SimpleDateFormat dateFormat = null;

	static String LOCALPATH_CDR = "";
	static TreeMap<String, String> numTelScon = null;
	static TreeMap<String, String> prefissiScon = null;
	static String numTelSconosciuti = "";
	static String prefissiSconosciuti = "";
	static String operatore = "";
	static String cdrErrati = "";
	static String estensione = "";

	// Database credentials
	static String USER = null;
	static String PASS = null;
	static Connection conn = null;
	Statement stmt = null;

	public static void initLoaderCdr() {
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			if (StringUtils.contains(JDBC_DRIVER, "postgresql"))
				conn.setAutoCommit(false);
			logger.info("Opened database successfully, start loading CDR " + operatore);
			utility.initHashMapTable(conn);
		} catch (SQLException se) {
			logger.error("SQLException:" + se.getMessage());
		} catch (Exception e) {
			logger.error("Exception:" + e.getMessage());
		}

	}// end main

	/**
	 * 
	 * @param conn
	 * @param pathdir
	 */
	public synchronized String loadCdr() {

		TreeMap<String, String> t = FileSystemUtility.getFileList(LOCALPATH_CDR, estensione);
		Set<Entry<String, String>> set = t.entrySet();
		Iterator<Entry<String, String>> iterator = set.iterator();

		FileInputStream fstream = null;
		BufferedReader br = null;
		Statement stmt = null;
		String esitoCaricamento = "\n***************** " + operatore.toUpperCase() + " ******************\n";
		String cdr = null;
		int contaFileCaricati = 0;

		try {

			while (iterator.hasNext()) {
				int countCdrFile = 0;
				long inifile = System.currentTimeMillis();

				Map.Entry<String, String> me = (Map.Entry<String, String>) iterator.next();
				String nomefile = me.getValue().toString();

				if (utility.isLoaded(conn, nomefile))
					continue;

				contaFileCaricati++;
				fstream = new FileInputStream(LOCALPATH_CDR + nomefile);

				br = new BufferedReader(new InputStreamReader(fstream));
				Long idfile = utility.insertCdrFile(conn, nomefile, operatore);

				stmt = conn.createStatement();

				if (StringUtils.equals(operatore, "plink") || StringUtils.equals(operatore, "bellnet")
						|| StringUtils.equals(operatore, "viatek"))
					cdr = br.readLine(); // salto la riga di intestazione

				while ((cdr = br.readLine()) != null) {

					String sql = getSqlParsingCdr(cdr, nomefile, idfile);

					if (StringUtils.isEmpty(sql))
						continue;

					countCdrFile++;
					stmt.executeUpdate(sql);

				}
				Timestamp now = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
				String sqlUpdate = "UPDATE cdr_file set num_cdr=" + countCdrFile + ", data_end_ins='" + now
						+ "' where idfile=" + idfile + ";";
				stmt.executeUpdate(sqlUpdate);

				if (StringUtils.contains(JDBC_DRIVER, "postgresql"))
					conn.commit();

				logger.info(idfile + "-" + nomefile + ":" + countCdrFile + ": "
						+ ((System.currentTimeMillis() - inifile) / 1000) + " sec.");

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
				if (conn != null)
					conn.close();

				if (contaFileCaricati > 0) {
					numTelSconosciuti = setReportFromHashMap(numTelScon,
							"\n" + operatore.toUpperCase() + ", numeri di telefono non riconosciuti:\n");
					prefissiSconosciuti = setReportFromHashMap(prefissiScon,
							"\n" + operatore.toUpperCase() + ", prefissi non riconosciuti:\n");
				} else {
					esitoCaricamento ="\n"+operatore.toUpperCase()+": nessun file caricato";
				}

			} catch (Exception e2) {
				logger.error("Exception e2:" + e2.getMessage());
				esitoCaricamento = "\nException e2:" + e2.getMessage();
			}
		}

		if (StringUtils.contains(esitoCaricamento, "Exception"))
			esitoCaricamento += operatore.toUpperCase() + ", errore caricamento CDR in database " + DB_URL + ":"
					+ esitoCaricamento + "\n";
		
		else if (contaFileCaricati > 0)
			esitoCaricamento += operatore.toUpperCase() + ", "+contaFileCaricati+" file CDR caricati correttamente in database \n";

		if (!"".equals(cdrErrati))
			cdrErrati = "CDR errati:\n" + cdrErrati;

		return esitoCaricamento + cdrErrati + numTelSconosciuti + prefissiSconosciuti
				+ "\n-----------------------------------------";
	}

	/**
	 * 
	 * @param tm
	 * @param tiporeport
	 * @return
	 */
	public static String setReportFromHashMap(TreeMap<String, String> tm, String tiporeport) {
		String rep = tiporeport;
		int i = 0;
		for (String key : tm.keySet()) {
			++i;
			logger.info(i + "-" + key + ", file:" + tm.get(key));
			rep += (i) + "-" + key + ", file:" + tm.get(key) + "\n";
		}

		return rep;
	}

	/**
	 * 
	 * @param st
	 * @param idfile
	 * @return
	 */
	public String getSqlParsingCdr(String cdr, String nomefile, Long idfile) {

		return null;
	}

	public static void setJDBC_DRIVER(String jDBC_DRIVER) {
		JDBC_DRIVER = jDBC_DRIVER;
	}

	public static void setDB_URL(String dB_URL) {
		DB_URL = dB_URL;
	}

	public static void setUSER(String uSER) {
		USER = uSER;
	}

	public static void setPASS(String pASS) {
		PASS = pASS;
	}

}//