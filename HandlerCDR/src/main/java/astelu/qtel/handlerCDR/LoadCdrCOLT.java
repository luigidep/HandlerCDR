package astelu.qtel.handlerCDR;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class LoadCdrCOLT extends LoadCdr {
	final static Logger logger = Logger.getLogger(LoadCdrCOLT.class.getName());
	/**
	 * 
	 * @param conn
	 * @param pathdir
	 */
	public LoadCdrCOLT() {
		sep=" ";
		NUMTOKEN_CDR = 4;
		LOCALPATH_CDR = "/opt/colt/";
		operatore = "colt";
		cdrErrati = "";
		numTelScon = new TreeMap<String, String>();
		prefissiScon = new TreeMap<String, String>();
		numTelSconosciuti = "";
		prefissiSconosciuti = "";
		estensione=".cdr";
		dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
		initLoaderCdr();

	}

	public String doWork() {

		return loadCdr();

	}

	/**
	 * 
	 * @param cdr
	 * @param nomefile
	 * @param idfile
	 * @return
	 */
	public String getSqlParsingCdr(String cdr, String nomefile, Long idfile) {

		String sql = null;
		try {

			StringTokenizer st = new StringTokenizer(cdr,sep);

			if (st.countTokens() != NUMTOKEN_CDR) {
				cdrErrati += "num tokens errato:" + st.countTokens() + ", file:" + nomefile + ", cdr:" + cdr
						+ "\n";
				logger.warn(cdrErrati);
				return null;
			}
			
			@SuppressWarnings("unused")
			String iduser = st.nextToken();
			String cte = st.nextToken();

			if (!utility.numTelClienti.containsKey(cte))
				numTelScon.put(cte, nomefile);

			String cto = st.nextToken();
			String dateAndDecimi = st.nextToken();
			String time = dateAndDecimi.substring(0, 14);
			Integer decimi = new Integer(dateAndDecimi.substring(16));
			String tipoCh_paese = utility.getTipoChiamataColt(cte, cto);
			String tc = StringUtils.substringBefore(tipoCh_paese, "_");
			String paese = StringUtils.substringAfter(tipoCh_paese, "_");
			paese = paese.replaceAll("'", "''");

			if (StringUtils.equals(tc, "UNKNOW")) {
				prefissiScon.put(cto, nomefile);
			}

			long tm = dateFormat.parse(time).getTime();
			Timestamp ts = new Timestamp(tm);
			// Integer secround = Math.round(new Integer(decimi) / 10f);
			Integer secsup = (int) Math.ceil(new Integer(decimi) / 10f);
			Integer secp1s = secsup + 1;
			sql = "INSERT INTO chiamateCOLT (idfile,telcli,teldestinazione,tipoChiamata,dataora,durata_effettiva,durata,durata_p1s,paese)"
					+ " VALUES (" + idfile + ",'" + cte + "','" + cto + "','" + tc + "','" + ts + "'," + decimi + ","
					+ secsup + "," + secp1s + ",'" + paese + "');";

		} catch (Exception e) {
			logger.error("exception e:" + e.getMessage());
			sql = null;
		}

		return sql;

	}
}