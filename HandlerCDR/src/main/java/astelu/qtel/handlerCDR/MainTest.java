package astelu.qtel.handlerCDR;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

public class MainTest {
	final static Logger log = Logger.getLogger(MainTest.class);

	public static void main(String[] args)
			throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException {
		// TODO Auto-generated method stub

		BasicConfigurator.configure();
		log.info("main Test");
		Properties prop = new Properties();
		InputStream input = MainCronSchedule_CDRHandler.class.getClassLoader().getResourceAsStream("config.properties");

		// load a properties file
		prop.load(input);
		// String textEmail = "\n" + LoadCdr_sqlexpress.LoadCdrHandler();
		String textEmail = "";
/*
		LoadCdr.setDB_URL(prop.getProperty("DB_URL"));
		LoadCdr.setJDBC_DRIVER(prop.getProperty("JDBC_DRIVER"));
		LoadCdr.setPASS(prop.getProperty("PASS"));
		LoadCdr.setUSER(prop.getProperty("USER"));
		String operatori = prop.getProperty("operatori");
		StringTokenizer st = new StringTokenizer(operatori);
		while (st.hasMoreTokens()) {
			String op = st.nextToken().toUpperCase();
			String className = "LoadCdr" + op;
			log.info("loadCdr:"+className);
			LoadCdr cdrLoader = (LoadCdr) Class.forName("astelu.qtel.handlerCDR."+className).newInstance();
			textEmail += cdrLoader.loadCdr();

		}
*/
		/*
		 * LoadCdrCOLT cdrColt= new LoadCdrCOLT(); String
		 * textEmail=cdrColt.doWork();
		 * 
		 * LoadCdrTISCALI cdrTiscali= new LoadCdrTISCALI();
		 * textEmail+=cdrTiscali.doWork();
		 * 
		 * 
		 * LoadCdrBELLNET cdrBellnet= new LoadCdrBELLNET();
		 * 
		 * textEmail+=cdrBellnet.doWork();
		 * 
		 * LoadCdrPLINK cdrPlink= new LoadCdrPLINK();
		 * textEmail+=cdrPlink.doWork();
		 * 
		 * LoadCdr cdrLoader= new LoadCdrVIATEK();
		 * textEmail+=cdrLoader.loadCdr();
		 */
		System.out.println("textEmail:\n" + textEmail);

	}

}
