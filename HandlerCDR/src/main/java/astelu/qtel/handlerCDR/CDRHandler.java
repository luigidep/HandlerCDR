package astelu.qtel.handlerCDR;

import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class CDRHandler implements Job {
	final static Logger log = Logger.getLogger(CDRHandler.class.getName());
    static String operatori="";
    final static String VIATEK="VIATEK";
    final static String COLT="COLT";
    final static String PLINK="PLINK";
    final static String BELLNET="BELLNET";
    final static String TISCALI="TISCALI";
	public void execute(JobExecutionContext context) throws JobExecutionException {
		String textEmail = "";
		try {
			log.info("***************INI JOB******************");
			log.info("---------get cdr via ftp connection-----");
			HashMap<String, String> hm = FTP_Colt_CDR_last.GetFtpFiles();
			Thread.sleep(300);
			textEmail = hm.get("textEmail");
			log.info("textEmail:" + textEmail);
			//int nfs = Integer.parseInt(hm.get("numFileScar"));

			// if (nfs > 0 && !StringUtils.contains(hm.get("textEmail"),
			// "Exception")) {
			// if (!StringUtils.contains(hm.get("textEmail"), "Exception")) {
			
			textEmail +="\n--------archiviazione file e caricamento cdr in database--------";
			
			StringTokenizer st = new StringTokenizer(operatori);
			String pack="astelu.qtel.handlerCDR.";
			while (st.hasMoreTokens()) {
				String op = st.nextToken().toUpperCase();
				String className = "LoadCdr" + op;
				log.info("loadCdr:"+className);
				switch (op) {
				case COLT:
					textEmail += "\n" + NetworkShareFileCopy.CopySmbFilesColt();
					Thread.sleep(150);
					break;
				case TISCALI:
					textEmail += "\n" + NetworkShareFileCopy.CopySmbFilesTiscali();
					Thread.sleep(150);
					break;
				default:
					textEmail += "\n" + NetworkShareFileCopy.CopySmbFilesFromRemToLoc(op);
					break;
				}
				LoadCdr cdrLoader = (LoadCdr) Class.forName(pack+className).newInstance();
				textEmail += cdrLoader.loadCdr();
			}

			log.info("-----------send Email-------------------");

			textEmail += "\nCDR operatori caricati:"+operatori;
			textEmail += "\n*** email inviata in modo automatico dal processo per la gestione dei CDR ***";
			new SendMailTLS().sendEmailFromSupport(
					"Download CDR Colt via FTP, caricamento in DB e controllo numeri clienti per i vari fornitori", textEmail);
			log.info("***************END JOB******************");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			log.error("InterruptedException" + e.getMessage());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("Exception e:" + e.getMessage());
		}

	}

}
