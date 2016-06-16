package astelu.qtel.handlerCDR;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.poifs.crypt.Decryptor;
import org.apache.poi.poifs.crypt.EncryptionInfo;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileInputStream;

public class LoadClienti_fromNetworkExcel {
	final static Logger logger = Logger.getLogger(LoadClienti_fromNetworkExcel.class.getName());

	static final String SMB_FILE_Servizi = "smb://server-hp//Documenti/Servizi/";
	static final String SMB_FILE_ArchivioContratti = "smb://server-hp/Documenti/Servizi/_Archivio tutti i contratti/";
	//static final String ARCHIVIO_CLIENTI = "Archivio Clienti.xlsx";
	static final String ARCHIVIO_CLIENTI = "ArchivioClienti20160517.rar";
	static final String LOCAL_PATH_ArchivioClienti = "/opt/archivio_clienti/";

	// JDBC driver name and database URL
	static final String JDBC_DRIVER = "net.sourceforge.jtds.jdbc.Driver";
	static final String DB_URL = "jdbc:jtds:sqlserver://server-hp/SQLEXPRESS;databaseName=Mexal_Telefonate";
	static final String DB_URL_dwpbi = "jdbc:jtds:sqlserver://server-hp/SQLEXPRESS;databaseName=DwPbi";

	// Database credentials
	static final String USER = "jdbc";
	static final String PASS = "jdbc";

	static final String JDBC_DRIVER_p = "org.postgresql.Driver";
	static final String DB_URL_p = "jdbc:postgresql://localhost:5432/astelu";

	static final String USER_p = "astelu";
	static final String PASS_p = "astelu";
	static final String USER_NAME = "server";
	static final String PASSWORD = "server";

	public static void main(String[] args) {
		Connection conn = null;
		Statement stmt = null;
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
		LinkedList<String> remoteFileDaCopiare = new LinkedList<String>();
		try {

			remoteFileDaCopiare.add(ARCHIVIO_CLIENTI);
			boolean isCopied = NetworkShareFileCopy.copyFiles(remoteFileDaCopiare, LOCAL_PATH_ArchivioClienti,
					SMB_FILE_ArchivioContratti);
			//boolean isCopied=true;
			if (isCopied) {
				Class.forName(JDBC_DRIVER_p);
				conn = DriverManager.getConnection(DB_URL_p, USER_p, PASS_p);
				// conn.setAutoCommit(false);
				System.out.println("Opened database successfully");

				File myFile = new File(LOCAL_PATH_ArchivioClienti + ARCHIVIO_CLIENTI);
				FileInputStream fis = new FileInputStream(myFile);

				XSSFWorkbook myWorkBook = new XSSFWorkbook(fis);
				System.out.println("new XSSFWorkbook");

				XSSFSheet mySheet = myWorkBook.getSheet("Wlr");

				Iterator<Row> rowIterator = mySheet.iterator(); // Traversing
																// over

				while (rowIterator.hasNext()) {
					Row row = rowIterator.next();

					Iterator<Cell> cellIterator = row.cellIterator();

					int i = 0;
					while (cellIterator.hasNext()) {

						Cell cell = cellIterator.next();
						if (i == 0 && StringUtils.isEmpty(cell.toString()))
							break;
						switch (cell.getCellType()) {
						case Cell.CELL_TYPE_STRING:
							String s = cell.getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();
							System.out.print("s:" + s + "\t");
							break;
						case Cell.CELL_TYPE_NUMERIC:
							if (DateUtil.isCellDateFormatted(cell)) {
								Date date = cell.getDateCellValue();
								LocalDate ldate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
								String sdate = ldate.format(formatter);
								System.out.print("date:" + sdate + "\t");

							} else {

								Double db = cell.getNumericCellValue();
								System.out.print("n:" + db + "\t");
							}

							break;
						case Cell.CELL_TYPE_BOOLEAN:
							System.out.print("b:" + cell.getBooleanCellValue() + "\t");
							break;
						case Cell.CELL_TYPE_BLANK:
							System.out.print("b:" + cell.getStringCellValue() + "\t");
						default:
							System.out.print("d:" + cell.getStringCellValue() + "\t");
						}
						++i;
					}

					System.out.println("i=" + i);
				}
				myWorkBook.close();
				fis.close();
				conn.close();
			}
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
	public static void loadClientiServizi(Connection conn) {

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
	public static void loadClientixDSL(Connection conn) {

		try {

			FileInputStream fstream = new FileInputStream(
					"/home/luigi/Documenti/work/astelutilities/IndirizziIPClienti.csv");
			BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
			String strLine;

			HashMap<String, Long> hm = utility.getClientiServizi(conn);
			Statement stmt = conn.createStatement();
			int i = 0;
			br.readLine();// salta la prima riga
			int contaNonTrovati = 0;
			while ((strLine = br.readLine()) != null) {

				System.out.println((++i) + "-line:" + strLine);
				String[] t = strLine.split(";");
				// if (t.length!=6)
				// logger.warn(strLine+"---length----------------------------------------="+t.length);

				int j = -1;
				String ragsoc = t[++j];
				String ip = t[++j];
				String ping = t[++j];
				String man = t[++j];
				String modrouter = t[++j];

				ragsoc = ragsoc.replaceAll("'", "''");
				Long idcliente = 0L;
				if (hm.containsKey(ragsoc)) {
					idcliente = hm.get(ragsoc);
					String sql = "INSERT INTO clientixdsl (idcliente,ip,ping,man,modello_router)" + " VALUES ('"
							+ idcliente + "','" + ip + "','" + ping + "','" + man + "','" + modrouter + "');";

					stmt.executeUpdate(sql);
				} else {
					contaNonTrovati++;
					logger.warn(contaNonTrovati + ": cliente non trovato:-------------------->" + ragsoc);

				}

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
