package astelu.qtel.handlerCDR;

import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;


public class MainCronSchedule_CDRHandler 
{
	final static Logger log = Logger.getLogger(MainCronSchedule_CDRHandler.class);
	//final static Logger log = (Logger) LoggerFactory.getLogger(MainCronSchedule_CDRHandler.class);

    public static void main( String[] args ) throws Exception
    {
    	BasicConfigurator.configure();
    	log.info("main start");
    	Properties prop = new Properties();

		//FileInputStream input = new FileInputStream("src/main/resources/config.properties");

		InputStream input= MainCronSchedule_CDRHandler.class.getClassLoader().getResourceAsStream("config.properties");
		
		// load a properties file
		prop.load(input);

		// get the property value and print it out
		String crontab=prop.getProperty("crontab");
		
		LoadCdr.setDB_URL(prop.getProperty("DB_URL"));
		LoadCdr.setJDBC_DRIVER(prop.getProperty("JDBC_DRIVER"));
		LoadCdr.setPASS(prop.getProperty("PASS"));
		LoadCdr.setUSER(prop.getProperty("USER"));
		CDRHandler.operatori=prop.getProperty("operatori");
		SendMailTLS.emails=prop.getProperty("emails");
		
		log.info("main start on db:"+LoadCdr.DB_URL);
		
		input.close();
 	
    	JobDetail job = JobBuilder.newJob(CDRHandler.class).withIdentity("CDRHandler", "astelu").build();
    	
    	//Trigger trigger = TriggerBuilder.newTrigger().withIdentity("CDRHandler", "astelu").startNow().build();
    	
    	Trigger trigger = TriggerBuilder
				.newTrigger()
				.withIdentity("CDRHandler", "astelu")
				.withSchedule(
						//CronScheduleBuilder.cronSchedule("0 2 * * * ?")) //ogni 2 minuti
				CronScheduleBuilder.cronSchedule(crontab)) //ogni GIORNO ALLE 12.18
				.build();
    	
    	//schedule it
    	Scheduler scheduler = new StdSchedulerFactory().getScheduler();
    	scheduler.start();
    	scheduler.scheduleJob(job, trigger);
    	log.info("job is scheduled and started, crontab process is:"+crontab);
    
    }
}
