package astelu.qtel.handlerCDR;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class LoadCdrTISCALI extends LoadCdr {
	final static Logger logger = Logger.getLogger(LoadCdrTISCALI.class.getName());
	/**
	 * 
	 * @param conn
	 * @param pathdir
	 */
	public LoadCdrTISCALI() {
		sep=";";
		NUMTOKEN_CDR = 6;
		LOCALPATH_CDR = "/opt/tiscali/";
		operatore = "tiscali";
		cdrErrati = "";
		numTelScon = new TreeMap<String, String>();
		prefissiScon = new TreeMap<String, String>();
		numTelSconosciuti = "";
		prefissiSconosciuti = "";
		estensione=".txt";
		dateFormat = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss");
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

			StringTokenizer st = new StringTokenizer(cdr, sep);
			if (st.countTokens() != NUMTOKEN_CDR) {
				cdrErrati += "num tokens errato:" + st.countTokens() + ", file:" + nomefile + ", cdr:" + cdr
						+ "\n";
				logger.warn(cdrErrati);
				return null;
			}


			String cte = st.nextToken();
			if (!utility.numTelClienti.containsKey(cte)) 
				numTelScon.put(cte, nomefile);
			
			
			String dataora = st.nextToken();
			long tm = dateFormat.parse(dataora).getTime();
			Timestamp ts = new Timestamp(tm);
			String cto = st.nextToken();
		
			
			String tipoCh_paese = utility.getTipoChiamataTiscali(cte, cto);
			String tc=StringUtils.substringBefore(tipoCh_paese, "_");
			String paese=(StringUtils.substringAfter(tipoCh_paese, "_")).replaceAll("'", "''");
			
			if(StringUtils.equals(tc, "UNKNOW")) {
				prefissiScon.put(cto, nomefile);
			}
			
			String durata = st.nextToken();
			@SuppressWarnings("unused")
			String user = st.nextToken();
			Double costo = new Double(st.nextToken());

			sql = "INSERT INTO chiamateTISCALI (idfile, telcli, teldestinazione, tipoChiamata, dataora, durata_effettiva, durata, costo, paese)"
					+ " VALUES(" + idfile + ",'" + cte + "','" + cto + "','" + tc + "','" + ts + "',"
					+ durata + "," + durata + "," + costo + ",'"+paese+"');";


		} catch (Exception e) {
			logger.error("exception e:" + e.getMessage());
			sql = null;
		}

		return sql;

	}
}