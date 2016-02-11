package astelu.qtel.handlerCDR;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

//smb://server-hp/astel/Servizi/FILE_COLT/Archivio%20telefonate/2015/07-luglio

import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbException;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbFileInputStream;
import jcifs.smb.SmbFileOutputStream;

public class NetworkShareFileCopy {
	static final String USER_NAME = "server";
	static final String PASSWORD = "server";
	// e.g. Assuming your network folder is: \my.myserver.netsharedpublicphotos
	// static final String NETWORK_FOLDER =
	// "smb://server-hp/astel/Servizi/FILE_COLT/Archivio%20telefonate/";
	// static final String NETWORK_FOLDER =
	// "smb://192.168.125.10/astel/Servizi/FILE_COLT/Archivio telefonate/";
	final static Logger logger = Logger.getLogger(NetworkShareFileCopy.class.getName());
	static final String LOCALPATH_FOLDER_COLT = "/opt/colt/";
	static final String LOCALPATH_FOLDER_TISCALI = "/opt/tiscali/";
	static final String LOCALPATH_FOLDER_VIATEK = "/opt/viatek/";
	static final String LOCALPATH_FOLDER_PLINK = "/opt/plink/";
	static final String LOCALPATH_FOLDER_BELLNET = "/opt/bellnet/";
	static final String NETWORK_FOLDER_COLT = "smb://server-hp/astel/Servizi/FILE_COLT/ftp_download/";
	static final String NETWORK_FOLDER_TISCALI = "smb://server-hp/astel/Servizi/FILE_TISCALI/download_cdr/";
	static final String NETWORK_FOLDER_VIATEK = "smb://server-hp/astel/Servizi/FILE_WLR/download_cdr_VIATEK/";
	static final String NETWORK_FOLDER_PLINK = "smb://server-hp/astel/Servizi/FILE_WLR/download_cdr_PLINK/";
	static final String NETWORK_FOLDER_BELLNET = "smb://server-hp/astel/Servizi/FILE_WLR/download_cdr_BELLNET/";

	// static final String NETWORK_FOLDER = "smb://galaxy/pubblici/";
	// static final String NETWORK_FOLDER = "smb://win7-64-vm/condivisa/";

	/**
	 * @param args
	 * @throws Exception
	 */
	public static String CopySmbFilesColt() throws Exception {
		// String fileContent = "This is a test file";
		// BasicConfigurator.configure();
		// new NetworkShareFileCopy().copyFiles(fileContent, "smb1.txt");
		listAvailableHosts(false);

		LinkedList<String> fileDaCopiare = new LinkedList<String>();
		String lastFileDownloaded = FileSystemUtility.getLastFile(LOCALPATH_FOLDER_COLT, ".cdr");
		logger.info("lastFileDownloaded:" + lastFileDownloaded);
		// getSambaFileList(NETWORK_FOLDER);
		// System.out.println("lastFileSamba :" +
		// getSambaFileTreeMap(NETWORK_FOLDER).lastKey().toString());

		// Long lastFile =
		// getSambaFileTreeMap(NETWORK_FOLDER).get(getSambaFileTreeMap(NETWORK_FOLDER).lastKey().toString());
		// System.out.println("long last file:" + lastFile);

		TreeMap<String, Long> tm = getSambaFileTreeMapColt(NETWORK_FOLDER_COLT);
		Long lastFile = tm.get(tm.lastKey());
		logger.info("lastSambaFile:" + lastFile);

		TreeMap<String, Long> tmFtp = FileSystemUtility.getFileListL(LOCALPATH_FOLDER_COLT);

		for (Map.Entry<String, Long> entry : tmFtp.entrySet()) {
			String nomeFile = entry.getKey();
			Long numFile = entry.getValue();
			if (numFile > lastFile)
				fileDaCopiare.add(nomeFile);

			// System.out.println(key + " => " + value);
		}
		if (fileDaCopiare.size() > 0)
			copyFilesColt(fileDaCopiare);

		return "COLT: "+fileDaCopiare.size()+" file archiviati correttamente nella directory del server:\nastel\\servizi\\FILE_COLT\\ftp_download\n";

	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static String CopySmbFilesTiscali() throws Exception {

		LinkedList<String> remoteFileDaCopiare = new LinkedList<String>();
		String lastLocalFileName = FileSystemUtility.getLastFile(LOCALPATH_FOLDER_TISCALI, ".txt");
		String lastMonthLocalFile = "";
		logger.info("lastLocalFile:" + lastLocalFileName);

		Long lastDayLocalFile = 0L;
		String md = "";
		if (StringUtils.isNotEmpty(lastLocalFileName)) {
			StringTokenizer st = new StringTokenizer(lastLocalFileName, "_");
			st.nextToken();
			md = st.nextToken();
			lastMonthLocalFile = StringUtils.substring(md, 0, 3);
			lastDayLocalFile = Long.parseLong(StringUtils.substring(md, 3));
		}

		TreeMap<String, Long> tm = getSambaFileTreeMapTiscali(NETWORK_FOLDER_TISCALI);

		String lastRemoteFileName = "";

		String lastMonthRemoteFile = "";
		// Long lastDayRemoteFile = 0L;

		if (!tm.isEmpty()) {
			StringTokenizer st = new StringTokenizer(tm.lastKey(), "_");
			st.nextToken();
			md = st.nextToken();
			lastMonthRemoteFile = StringUtils.substring(md, 0, 3);
			// lastDayRemoteFile = tm.get(tm.lastKey());
			lastRemoteFileName = tm.lastKey();
		}

		logger.info("lastRemoteFileName:" + lastRemoteFileName);

		// TreeMap<String, Long> tmFtp =
		// FileSystemUtility.getFileListL(LOCALPATH_CDR_COLT);
		if (!StringUtils.equals(lastMonthRemoteFile, lastMonthLocalFile))
			lastDayLocalFile = 0L;

		for (Map.Entry<String, Long> entry : tm.entrySet()) {
			String nomeFile = entry.getKey();
			Long numFile = entry.getValue();
			if (numFile > lastDayLocalFile)
				remoteFileDaCopiare.add(nomeFile);

			// System.out.println(key + " => " + value);
		}
		if (remoteFileDaCopiare.size() > 0)
			copyFiles(remoteFileDaCopiare, LOCALPATH_FOLDER_TISCALI, NETWORK_FOLDER_TISCALI);

		return "TISCALI: "+remoteFileDaCopiare.size()+" file copiati correttamente e pronti al caricamento\n";

	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static String CopySmbFilesFromRemToLoc(String op) throws Exception {

		String locPath = "";
		String remPath = "";

		switch (op) {
		case CDRHandler.PLINK:
			locPath = LOCALPATH_FOLDER_PLINK;
			remPath = NETWORK_FOLDER_PLINK;
			break;
		case CDRHandler.BELLNET:
			locPath = LOCALPATH_FOLDER_BELLNET;
			remPath = NETWORK_FOLDER_BELLNET;
			break;

		case CDRHandler.VIATEK:
			locPath = LOCALPATH_FOLDER_VIATEK;
			remPath = NETWORK_FOLDER_VIATEK;
			break;
		default:
			break;
		}

		LinkedList<String> remoteFileDaCopiare = new LinkedList<String>();
		TreeMap<String, String> localFileList = FileSystemUtility.getFileList(locPath, ".txt");

		TreeMap<String, String> remoteFileList = getSambaFileTreeMap(remPath);

		for (Map.Entry<String, String> rf : remoteFileList.entrySet())
			if (!localFileList.containsValue(rf.getKey()))
				remoteFileDaCopiare.add(rf.getKey());

		if (remoteFileDaCopiare.size() > 0)
			copyFiles(remoteFileDaCopiare, locPath, remPath);

		return op.toUpperCase()+": "+remoteFileDaCopiare.size()+" file copiati correttamente e pronti al caricamento\n";

	}

	public boolean copyFiles(String fileContent, String fileName) {
		boolean successful = false;
		try {
			String user = USER_NAME + ":" + PASSWORD;
			System.out.println("User: " + user);

			NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication(user);
			String path = NETWORK_FOLDER_COLT + fileName;
			System.out.println("Path: " + path);

			SmbFile sFile = new SmbFile(path, auth);
			SmbFileOutputStream sfos = new SmbFileOutputStream(sFile);
			sfos.write(fileContent.getBytes());
			sfos.close();
			// copyFileUsingJcifs(domain,userName, password, sourcePath,
			// destinationPath);

			successful = true;
			System.out.println("Successful:" + successful);
		} catch (Exception e) {
			successful = false;
			e.printStackTrace();
		}
		return successful;
	}

	public static boolean copyFilesColt(LinkedList<String> files) {
		boolean successful = false;
		try {
			String user = USER_NAME + ":" + PASSWORD;
			System.out.println("User: " + user);

			// NtlmPasswordAuthentication auth = new
			// NtlmPasswordAuthentication(user);
			SmbFile sFile = null;
			SmbFileOutputStream sfos = null;
			for (Iterator<String> iterator = files.iterator(); iterator.hasNext();) {

				String fn = (String) iterator.next();
				logger.info("copia file :" + fn);
				String path = NETWORK_FOLDER_COLT + fn;

				// sFile = new SmbFile(path, auth);
				sFile = new SmbFile(path);
				sfos = new SmbFileOutputStream(sFile);
				sfos.write(readFile(LOCALPATH_FOLDER_COLT + fn, Charset.defaultCharset()).getBytes());

				sfos.close();
				Thread.sleep(200);
			}

			successful = true;
			logger.info("file copiati correttamente");
		} catch (Exception e) {
			successful = false;
			logger.error("Exception e:" + e.getMessage());
		}
		return successful;
	}

	/**
	 * 
	 * @param path
	 * @param encoding
	 * @return
	 * @throws IOException
	 */
	static String readFile(String path, Charset encoding) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
	}

	public static String[] listAvailableHosts(boolean withIp) {
		List<String> hostNames = new ArrayList<String>();
		try {
			SmbFile[] workgroups = new SmbFile("smb://").listFiles();
			for (int i = 0; i < workgroups.length; i++) {
				try {
					SmbFile[] hosts = workgroups[i].listFiles();
					for (int j = 0; j < hosts.length; j++) {
						String name = hosts[j].getName();
						System.out.println("host:" + name);

						/*
						 * String nameWithoutSlash = name.substring(0,
						 * name.length() - 1); hostNames.add(nameWithoutSlash);
						 * if (withIp &&
						 * !IP_PATTERN.matcher(nameWithoutSlash).matches()) {
						 * try {
						 * hostNames.add(InetAddress.getByName(nameWithoutSlash)
						 * .getHostAddress()); } catch (UnknownHostException e)
						 * { } }
						 */
					}
				} catch (SmbException e) {
				}
			}
		} catch (SmbException e) {
		} catch (MalformedURLException e) {
			throw new RuntimeException(e);
		}
		String[] hosts = hostNames.toArray(new String[0]);
		return hosts;
	}
	/*
	 * public void pullUp(String from,String to) throws IOException,
	 * InterruptedException { SmbFile src=$(from); SmbFile dst=$(to); for (
	 * SmbFile e : src.listFiles()) { e.renameTo(new SmbFile(dst,e.getName()));
	 * } src.delete(); }
	 */

	/**
	 *
	 * @param path
	 * @return
	 * @throws java.lang.Exception
	 */
	public static LinkedList<String> getSambaFileList(String path) throws Exception {
		LinkedList<String> fList = new LinkedList<String>();

		// String user = "server:server";
		// System.out.println("User: " + user);

		// NtlmPasswordAuthentication auth = new
		// NtlmPasswordAuthentication(user);
		SmbFile[] sf = null;
		try {
			sf = new SmbFile(path).listFiles();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// SmbFile[] fArr = f.listFiles();
		TreeMap<String, Long> tm = new TreeMap<String, Long>();

		for (int i = 0; i < sf.length; i++) {
			fList.add(sf[i].getName());

			System.out.println(sf[i].getName());
			String fn = sf[i].getName();
			tm.put(fn, Long.parseLong(StringUtils.substringBefore(fn, ".cdr")));
		}

		return fList;
	}

	/**
	 * 
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public static TreeMap<String, Long> getSambaFileTreeMapTiscali(String path) throws Exception {

		TreeMap<String, Long> tm = new TreeMap<String, Long>();
		SmbFile[] sf = null;
		try {

			sf = new SmbFile(path).listFiles();

		} catch (Exception e) {
			logger.error("Exception e:" + e.getMessage());
		}

		for (int a = 0; a < sf.length; a++) {
			if (sf[a].getName().endsWith(".txt")) {
				StringTokenizer st = new StringTokenizer(sf[a].getName(), "_");
				st.nextToken();
				String md = st.nextToken();
				tm.put(sf[a].getName(), Long.parseLong(StringUtils.substring(md, 3)));
			}
		}

		return tm;
	}

	/**
	 * 
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public static TreeMap<String, String> getSambaFileTreeMap(String path) throws Exception {

		TreeMap<String, String> tm = new TreeMap<String, String>();
		SmbFile[] sf = null;
		try {

			sf = new SmbFile(path).listFiles();

		} catch (Exception e) {
			logger.error("Exception e:" + e.getMessage());
		}

		for (int a = 0; a < sf.length; a++) {
			if (sf[a].getName().endsWith(".txt")) {
				StringTokenizer st = new StringTokenizer(sf[a].getName(), "_");
				st.nextToken();
				String md = st.nextToken();
				tm.put(sf[a].getName(), sf[a].getName());
			}
		}

		return tm;
	}

	/**
	 * 
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public static TreeMap<String, Long> getSambaFileTreeMapTiscaliMonth(String path) throws Exception {

		TreeMap<String, Long> tm = new TreeMap<String, Long>();
		SmbFile[] sf = null;
		try {

			sf = new SmbFile(path).listFiles();

		} catch (Exception e) {
			logger.error("Exception e:" + e.getMessage());
		}

		for (int a = 0; a < sf.length; a++) {
			if (sf[a].getName().endsWith(".txt")) {
				StringTokenizer st = new StringTokenizer(sf[a].getName(), "_");
				st.nextToken();
				String md = st.nextToken();
				String month = md.substring(0, 3).toUpperCase();
				String currentMonth = StringUtils.substring(LocalDate.now().getMonth().name(), 0, 3);

				if (currentMonth.equals(month))
					tm.put(sf[a].getName(), Long.parseLong(StringUtils.substring(md, 3)));
			}
		}

		return tm;
	}

	/**
	 * 
	 * @param path
	 * @return
	 * @throws Exception
	 */
	public static TreeMap<String, Long> getSambaFileTreeMapColt(String path) throws Exception {

		TreeMap<String, Long> tm = new TreeMap<String, Long>();
		SmbFile[] sf = null;
		try {

			sf = new SmbFile(path).listFiles();

		} catch (Exception e) {
			e.printStackTrace();
		}

		for (int a = 0; a < sf.length; a++)
			tm.put(sf[a].getName(), Long.parseLong(StringUtils.substringBefore(sf[a].getName(), ".cdr")));

		return tm;
	}

	/**
	 * 
	 * @param files
	 * @return
	 */
	public static boolean copyFiles(LinkedList<String> files, String locPath, String remPath) {
		boolean successful = false;
		StringBuilder builder = null;
		try {
			String user = USER_NAME + ":" + PASSWORD;
			NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication(user);

			for (Iterator<String> iterator = files.iterator(); iterator.hasNext();) {

				String fn = (String) iterator.next();
				logger.info("copia file :" + fn);
				String path = remPath + fn;
				SmbFile sFile = new SmbFile(path, auth);
				builder = new StringBuilder();
				builder = readFileContent(sFile, builder);

				Files.write(Paths.get(locPath + fn), builder.toString().getBytes());

				/*
				 * System.out.println("========================== display " + fn
				 * + " =============="); System.out.println(builder.toString());
				 * System.out.println(
				 * "========================== End  here ================================"
				 * ); successful = true;
				 */
				Thread.sleep(100);
			}

			successful = true;
		} catch (Exception exception) {
			exception.printStackTrace();
		}
		return successful;
	}

	private static StringBuilder readFileContent(SmbFile sFile, StringBuilder builder) {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(new SmbFileInputStream(sFile)));
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		String lineReader = null;
		{
			try {
				while ((lineReader = reader.readLine()) != null) {
					builder.append(lineReader).append("\n");
				}
			} catch (IOException exception) {
				exception.printStackTrace();
			} finally {
				try {
					reader.close();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		}
		return builder;
	}

}
