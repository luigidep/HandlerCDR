package astelu.qtel.handlerCDR;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class LoadCdrPLINK extends LoadCdr {
	final static Logger logger = Logger.getLogger(LoadCdrPLINK.class.getName());
	/**
	 * 
	 * @param conn
	 * @param pathdir
	 */
	public LoadCdrPLINK() {
		sep="\t";
		NUMTOKEN_CDR = 8;
		LOCALPATH_CDR = "/opt/plink/";
		operatore = "plink";
		cdrErrati = "";
		numTelScon = new TreeMap<String, String>();
		prefissiScon = new TreeMap<String, String>();
		numTelSconosciuti = "";
		prefissiSconosciuti = "";
		estensione=".txt";
		dateFormat = new SimpleDateFormat("dd/MM/yyyy HH.mm.ss");
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
				cdrErrati = "num tokens errato:" + st.countTokens() + ", file:" + nomefile + ", cdr:" + cdr
						+ "\n";
				logger.warn(cdrErrati);
				return null;
			}
			
			String dataora = st.nextToken();
			long tm = dateFormat.parse(dataora).getTime();
			Timestamp ts = new Timestamp(tm);
			@SuppressWarnings("unused")
			String seqNum=st.nextToken();
			String durata = st.nextToken();
			String cte=StringUtils.substring(st.nextToken(), 2);
			
			if (!utility.numTelClienti.containsKey(cte))
				numTelScon.put(cte, nomefile);
			
			String cto = st.nextToken();
			String idServizioWlr=st.nextToken();
			String descrizione=st.nextToken();

			String c=StringUtils.replace(st.nextToken(), ",", ".");
			Double costo = new Double(c);
			
			String tipoCh_paese = utility.getTipoChiamata_WLR_VOIP(cte, cto);
			String tc=StringUtils.substringBefore(tipoCh_paese, "_");
			String paese=(StringUtils.substringAfter(tipoCh_paese, "_")).replaceAll("'", "''");
			
			if(StringUtils.equals(tc, "UNKNOW")) 
				prefissiScon.put(cto, nomefile);

			sql = "INSERT INTO chiamatePLINK (idfile, telcli, teldestinazione, tipoChiamata, paese, descrizioneplink, dataora, durata, costo, idservizioWLR)"
					+ " VALUES(" + idfile + ",'" + cte + "','" + cto + "','" + tc + "','" + paese + "','" + descrizione +"','" + ts + "',"
					+ durata + "," + costo + ",'"+idServizioWlr+"');";

		} catch (Exception e) {
			logger.error("exception e:" + e.getMessage());
			sql = null;
		}

		return sql;

	}
}