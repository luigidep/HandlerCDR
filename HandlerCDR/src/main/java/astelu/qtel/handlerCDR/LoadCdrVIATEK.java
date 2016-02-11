package astelu.qtel.handlerCDR;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class LoadCdrVIATEK extends LoadCdr {
	final static Logger logger = Logger.getLogger(LoadCdrVIATEK.class.getName());
	/**
	 * 
	 * @param conn
	 * @param pathdir
	 */
	public LoadCdrVIATEK() {
		sep="\t";
		NUMTOKEN_CDR = 5;
		LOCALPATH_CDR = "/opt/viatek/";
		operatore = "viatek";
		cdrErrati = "";
		numTelScon = new TreeMap<String, String>();
		prefissiScon = new TreeMap<String, String>();
		numTelSconosciuti = "";
		prefissiSconosciuti = "";
		estensione=".txt";
		dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
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
			if (st.countTokens() != NUMTOKEN_CDR && st.countTokens() != (NUMTOKEN_CDR+1)) {
				cdrErrati += "num tokens errato:" + st.countTokens() + ", file:" + nomefile + ", cdr:" + cdr
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
			
			String tipoCh_paese = utility.getTipoChiamata_WLR_VOIP(cte, cto);
			String tc=StringUtils.substringBefore(tipoCh_paese, "_");
			String paese=(StringUtils.substringAfter(tipoCh_paese, "_")).replaceAll("'", "''");
			String idservzioWlr="";
			if(st.countTokens() == 1) {
				idservzioWlr=st.nextToken();
			}
			String nverdeWlr="";

			
			if(StringUtils.equals(tc, "UNKNOW")) 
				prefissiScon.put(cto, nomefile);
			

			sql = "INSERT INTO chiamateVIATEK (idfile, telcli, teldestinazione, tipoChiamata, paese, dataora, durata, idservizioWlr,nverdeWlr)"
					+ " VALUES(" + idfile + ",'" + cte + "','" + cto + "','" + tc + "','" + paese +"','" + ts + "',"
					+ durata + ",'" + idservzioWlr + "','" + nverdeWlr+ "');";
			//logger.info("sql:"+sql);


		} catch (Exception e) {
			logger.error("exception e:" + e.getMessage());
			sql = null;
		}

		return sql;

	}
}