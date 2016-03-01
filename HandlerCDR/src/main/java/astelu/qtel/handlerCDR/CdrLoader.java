package astelu.qtel.handlerCDR;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

public class CdrLoader {
	final static Logger log = Logger.getLogger(CdrLoader.class.getName());
	final static String VIATEK = "VIATEK";
	final static String COLT = "COLT";
	final static String PLINK = "PLINK";
	final static String BELLNET = "BELLNET";
	final static String TISCALI = "TISCALI";

	public static String load(String operatore) {
		String textEmail = "";
		try {
			log.info("***************Load CDR******************");
			log.info("operatore:" + operatore);

			BasicConfigurator.configure();
	    	log.info("main start");
	    	Properties prop = new Properties();
			InputStream input= MainCronSchedule_CDRHandler.class.getClassLoader().getResourceAsStream("config.properties");
			
			// load a properties file
			prop.load(input);
			LoadCdr.setDB_URL(prop.getProperty("DB_URL"));
			LoadCdr.setJDBC_DRIVER(prop.getProperty("JDBC_DRIVER"));
			LoadCdr.setPASS(prop.getProperty("PASS"));
			LoadCdr.setUSER(prop.getProperty("USER"));
			//CDRHandler.operatori=prop.getProperty("operatori");
			SendMailTLS.emails=prop.getProperty("emails");
			
			input.close();
			
			
			

			String pack = "astelu.qtel.handlerCDR.";
			operatore = operatore.toUpperCase();
			String className = "LoadCdr" + operatore;
			
            String strArchiviazione="\n--------archiviazione file e caricamento cdr in database--------\n";
			
			switch (operatore) {
			case COLT:
				HashMap<String, String> hm = FTP_Colt_CDR_last.GetFtpFiles();
				textEmail = hm.get("textEmail")+"\n";
				//textEmail += strArchiviazione+NetworkShareFileCopy.CopySmbFilesColt();
				Thread.sleep(150);
				break;
			case TISCALI:
				//textEmail += strArchiviazione + NetworkShareFileCopy.CopySmbFilesTiscali();
				Thread.sleep(150);
				break;
			default:
				//textEmail += strArchiviazione + NetworkShareFileCopy.CopySmbFilesFromRemToLoc(operatore);
				Thread.sleep(150);
				break;
			}
		
			
			//LoadCdr cdrLoader = (LoadCdr) Class.forName(pack + className).newInstance();
			//textEmail += cdrLoader.loadCdr();

			log.info("-----------send Email-------------------");

			textEmail += "\nCDR operatore caricato:" + operatore;
			textEmail += "\n*** email inviata in modo automatico dal processo per la gestione dei CDR ***";
			// new SendMailTLS().sendEmailFromSupport(
			// "Download CDR Colt via FTP, caricamento in DB e controllo numeri
			// clienti per i vari fornitori", textEmail);
			log.info("***************END load:"+operatore);
		
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("Exception e:" + e.getMessage());
		}
		return "end load:"+operatore;

	}

}
