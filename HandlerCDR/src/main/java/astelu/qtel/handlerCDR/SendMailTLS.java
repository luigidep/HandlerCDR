package astelu.qtel.handlerCDR;

import java.util.Properties;
import java.util.StringTokenizer;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.log4j.Logger;

public class SendMailTLS {

	final static Logger logger = Logger.getLogger(SendMailTLS.class);

	static final String username = "supporto@astelutilities.com";
	static final String password = "support0";
	static String emails = "";

	public static void main(String[] args) {

		new SendMailTLS().sendEmailFromSupport("subject", "text");

	}

	public Session getSession() {
		Session s = null;
		try {

			Properties props = new Properties();
			props.put("mail.smtp.auth", "true");
			props.put("mail.smtp.starttls.enable", "true");
			props.put("mail.smtp.host", "smtp.gmail.com");
			props.put("mail.smtp.port", "587");

			s = Session.getInstance(props, new javax.mail.Authenticator() {
				protected PasswordAuthentication getPasswordAuthentication() {
					return new PasswordAuthentication(username, password);
				}
			});
		} catch (Exception e) {
			logger.error("Exception e:" + e.getMessage());
		}
		return s;
	}

	/**
	 * 
	 * @param subject
	 * @param text
	 */
	public void sendEmailFromSupport(String subject, String text) {
		try {

			Message message = new MimeMessage(getSession());
			message.setFrom(new InternetAddress("supporto@astelutilities.com"));

			StringTokenizer st = new StringTokenizer(emails);
			if (st.countTokens() > 0) {
				while (st.hasMoreTokens()) {
					String email = st.nextToken();
					message.addRecipients(Message.RecipientType.TO, InternetAddress.parse(email));
				}

				message.setSubject(subject);
				message.setText(text);

				Transport.send(message);

				logger.info("Email inviata");
			} else {
				logger.info("email non configurate");
			}
		} catch (MessagingException e) {
			logger.error("MessagingException e:" + e.getMessage());
			throw new RuntimeException(e);
		}
	}

}