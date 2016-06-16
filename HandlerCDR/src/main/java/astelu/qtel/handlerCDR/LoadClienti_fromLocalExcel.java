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

public class LoadClienti_fromLocalExcel {
	final static Logger logger = Logger.getLogger(LoadClienti_fromLocalExcel.class.getName());

	static final String SMB_FILE_Servizi = "smb://server-hp//Documenti/Servizi/";
	static final String SMB_FILE_ArchivioContratti = "smb://server-hp/Documenti/Servizi/_Archivio tutti i contratti/";
	//static final String ARCHIVIO_CLIENTI = "ArchivioClienti20160517.xlsx";
	static final String ARCHIVIO_CLIENTI = "Archivio ClientiRif.xlsx";
	static final String LOCAL_PATH_ArchivioClienti = "/opt/archivio_clienti/";
	//static final String LOCAL_PATH_ArchivioClienti = "/home/luigi/Documenti/work/astelutilities/";

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
	//static final String USER_NAME = "server";
	//static final String PASSWORD = "server";

	public static void main(String[] args) {
		Connection conn = null;
		Statement stmt = null;
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
		// LinkedList<String> remoteFileDaCopiare = new LinkedList<String>();
		try {

			Class.forName(JDBC_DRIVER_p);
			conn = DriverManager.getConnection(DB_URL_p, USER_p, PASS_p);
			//Class.forName(JDBC_DRIVER);
			//conn = DriverManager.getConnection(DB_URL, USER, PASS);
			// conn.setAutoCommit(false);
			System.out.println("Opened database successfully");

			File myFile = new File(LOCAL_PATH_ArchivioClienti + ARCHIVIO_CLIENTI);
			FileInputStream fis = new FileInputStream(myFile);

			XSSFWorkbook myWorkBook = new XSSFWorkbook(fis);
			//loadClientiServizi(conn, myWorkBook);
			loadClientiXDSL(conn, myWorkBook);

			myWorkBook.close();
			fis.close();
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
	public static void loadClientiServizi(Connection conn, XSSFWorkbook myWorkBook) {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
			XSSFSheet mySheet = myWorkBook.getSheet("Clienti");

			Iterator<Row> rowIterator = mySheet.iterator(); // Traversing
															// over
			int countrow = 0;
			int countIdmexalNull=0;
			Row row = rowIterator.next();
			while (rowIterator.hasNext()) {
				row = rowIterator.next();
				countrow++;
				System.out.print("row:" + countrow + "-");
				Iterator<Cell> cellIterator = row.cellIterator();

				//int i = 0;

				String ragsoc = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();
				String idmexal = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();
				String telefono = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();
				String cps = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();
				Date cps_att = cellIterator.next().getDateCellValue();
				String scps_att=null;
				if(cps_att != null) {
					LocalDate ldate = cps_att.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
					scps_att = "'"+ldate.format(formatter)+"'";
				}


				String adsl = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();

				String wlr = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();
				String voip = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();
				String note = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();
				String agente = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();

				ragsoc = ragsoc.replaceAll("'", "''");
				note = note.replaceAll("'", "''");
				agente = agente.replaceAll("'", "''");

				
				
				String sql = "INSERT INTO clientiservizi_cm (ragsoc,idmexal,telefono,cps, cps_att, adsl,wlr,voip,note,agente)"
				//String sql = "INSERT INTO clientiservizi (ragsoc,idmexal,telefono,cps, cps_att, adsl,wlr,voip,note,agente)"
						
						+ " VALUES ('" + ragsoc + "','" + idmexal + "','" + telefono + "','" + cps + "',"
						+ scps_att + ",'" + adsl + "','" + wlr + "','" + voip + "','" + note + "','" + agente
						+ "');";
				System.out.println("sql:" + sql);
				
				if (StringUtils.isNotEmpty(idmexal))
					stmt.executeUpdate(sql);
				else {
					System.out.println("ragsoc:"+ragsoc+" <--- id mexal null");
					countIdmexalNull++;
				}
			}

			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se) {
				logger.error("Exception se:" + se.getMessage());
			} // nothing we can do

			// conn.commit();
			System.out.println("count idMexal null:"+countIdmexalNull);

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
	public static void loadClientiXDSL(Connection conn, XSSFWorkbook myWorkBook) {
		Statement stmt = null;
		String ragsoc=null;
		try {
			stmt = conn.createStatement();
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
			XSSFSheet mySheet = myWorkBook.getSheet("Xdsl");

			Iterator<Row> rowIterator = mySheet.iterator(); // Traversing
															// over
			int countrow = 0;
			int countIdmexalNull=0;
			Row row = rowIterator.next();
			HashMap<String, Long> hm = utility.getClientiServizi(conn);
			Long idcliente = 0L;
			int contaNonTrovati=0;
			while (rowIterator.hasNext()) {
				row = rowIterator.next();
				countrow++;
				System.out.print("row:" + countrow + "-");
				Iterator<Cell> cellIterator = row.cellIterator();

				//int i = 0;

				ragsoc = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();
				String ragsoc1 = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();
				String oper = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();
				String taglio = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();
				String parametri = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();
				String ip = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();
				String tgu = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();
				String ping = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();
				String man = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();
				String router = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();
				String numAppoggio = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();
				String note = cellIterator.next().getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();


				
				if (hm.containsKey(ragsoc)) {
					idcliente = hm.get(ragsoc);
					ragsoc = ragsoc.replaceAll("'", "''");
					router = router.replaceAll("'", "''");
					note = note.replaceAll("'", "''");
					String sql = "INSERT INTO clientixdsl_cm1(idcliente,ragsoc, operatore, taglio, ip, tgu, ping, man, router, num_appoggio, note)"
							//String sql = "INSERT INTO clientiservizi (ragsoc,idmexal,telefono,cps, cps_att, adsl,wlr,voip,note,agente)"
									
									+ " VALUES ("+idcliente+",'" + ragsoc + "','" + oper + "','" + taglio + "','" + ip + "','"
									+ tgu + "','" + ping + "','" + man + "','" + router + "','" + numAppoggio + "','" + note
									+ "');";
							System.out.println("sql:" + sql);

					stmt.executeUpdate(sql);
				} else {
					contaNonTrovati++;
					logger.warn(contaNonTrovati + ": cliente non trovato:-------------------->" + ragsoc);

				}
				
				/*
				if (StringUtils.isEmpty(ragsoc))
					break;
				else
					stmt.executeUpdate(sql);
					*/
			}

			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se) {
				logger.error("Exception se:" + se.getMessage());
			} // nothing we can do

			// conn.commit();
			System.out.println("count idMexal null:"+countIdmexalNull);

		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Exception e:" + e.getMessage()+"-"+ragsoc);

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
