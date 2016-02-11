package astelu.qtel.handlerCDR;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.log4j.Logger;

/**
 * A program demonstrates how to upload files from local computer to a remote
 * FTP server using Apache Commons Net API.
 * 
 * @author www.codejava.net
 */
public class FTP_Colt_CDR_last {

	final static Logger logger = Logger.getLogger(FTP_Colt_CDR_last.class.getName());

	// static final String LOCALPATH="/home/luigi/Pubblici/Colt/2015/ftp/";
	static final String LOCALPATH_FTP = "/opt/colt/ftp/";
	static final String LOCALPATH_CDR = "/opt/colt/";

	/**
	 * 
	 */
	public static HashMap<String,String> GetFtpFiles() {

		
		HashMap<String,String> hm= new HashMap<String,String>();
		String server = "10.49.11.252";
		int port = 21;
		String user = "IT871";
		String pass = "uyI9KWlf";
		logger.info("main");
		FTPClient ftpClient = new FTPClient();
		String textEmail = "Connessione FTP Colt per download CDR File in data: " + LocalDateTime.now() + "\n";
		boolean success = false;
		try {

			int numfileScaricati=0;
			hm.put("numFileScar", ""+numfileScaricati);
			
			ftpClient.connect(server, port);
			ftpClient.login(user, pass);
			ftpClient.enterLocalPassiveMode();
			ftpClient.setFileType(FTP.BINARY_FILE_TYPE);

			String coltFtpPath = "/cdr/";

			String remoteFilename = null;// coltFtpPath+filename;
			File downloadFile = null; // new File(LOCALPATH_FTP+filename);

			FTPFile[] files = ftpClient.listFiles("cdr/");
			// System.out.println("file size:" + files.length);
			// String lastFileLoaded = getLastLoaded();
			String lastFileDownloaded = FileSystemUtility.getLastFile(LOCALPATH_CDR,".cdr");
			logger.debug("ultimo file scaricato:" + lastFileDownloaded);
			long lastFileNumber = Long.parseLong(StringUtils.substringBefore(lastFileDownloaded, ".cdr"));
			logger.debug("ftp files trovati:" + files.length);

			textEmail += "File scaricati via FTP:\n";
            
			
			for (FTPFile file : files) {

				success = false;
				long fileNumber = Long.parseLong(StringUtils.substringBefore(file.getName(), ".cdr"));
				if (fileNumber > lastFileNumber) {
					downloadFile = new File(LOCALPATH_FTP + file.getName());
					OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(downloadFile));
					remoteFilename = coltFtpPath + file.getName();
					success = ftpClient.retrieveFile(remoteFilename, outputStream);
					outputStream.close();
					numfileScaricati++;
					if (success) {
						// System.out.println("file.getName():"+file.getName());
						gunzipIt(file.getName());
						// System.out.println("download successfully and unzip
						// remote ftp file:" + file.getName());
						logger.info("file scaricato correttamente:" + file.getName());
						textEmail += file.getName() + "\n";

					}
				}

			}
			textEmail+="Numero di file scaricati:"+numfileScaricati+"\n\n";
			hm.put("numFileScar", ""+numfileScaricati);

		} catch (Exception e) {
			logger.error("Exception e:" + e.getMessage());
			textEmail += "Exception e:" + e.getMessage() + "\n";
		} finally {
			try {
				if (ftpClient.isConnected()) {
					ftpClient.logout();
					ftpClient.disconnect();
				}
			} catch (IOException ex) {
				logger.error("Exception ex:" + ex.getMessage());
			}
		}
		hm.put("textEmail", textEmail);
		return hm;

	}

	/**
	 * GunZip it
	 */
	public static void gunzipIt(String INPUT_GZIP_FILE) {

		byte[] buffer = new byte[1024];

		String OUTPUT_FILE = StringUtils.substringBefore(INPUT_GZIP_FILE, ".gz");

		try {

			GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(LOCALPATH_FTP + INPUT_GZIP_FILE));

			FileOutputStream out = new FileOutputStream(LOCALPATH_CDR + OUTPUT_FILE);

			int len;
			while ((len = gzis.read(buffer)) > 0) {
				out.write(buffer, 0, len);
			}

			gzis.close();
			out.close();

			// System.out.println("Done");

		} catch (Exception ex) {
			logger.error("exception:" + ex.getMessage());
		}
	}

}