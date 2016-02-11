package astelu.qtel.handlerCDR;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class LoadCdrBELLNET extends LoadCdr {
	final static Logger logger = Logger.getLogger(LoadCdrBELLNET.class.getName());
	/**
	 * 
	 * @param conn
	 * @param pathdir
	 */
	public LoadCdrBELLNET() {
		sep="\t";
		NUMTOKEN_CDR = 7;
		LOCALPATH_CDR = "/opt/bellnet/";
		operatore = "bellnet";
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

			

			//esempio cdr: 29/09/2015 08.31.21	314	32	390438370521	390299371062	800066703	70030	2
			String cto = st.nextToken();
			String codiceBellnet=st.nextToken();
			String idServizioWlr=st.nextToken();
			String nverdeWlr="";
			if(st.countTokens() == 1) {
				nverdeWlr=codiceBellnet;
				idServizioWlr=st.nextToken();
			}

			//se idserviziowlr = 2 significa che c'è un numero verde e quindi non devo considerare il numero sconosciuto
	
			
			if (!StringUtils.equals(idServizioWlr,"2") && !utility.numTelClienti.containsKey(cte)) {
				// numTelSconosciuti+=cte+"\n";
				numTelScon.put(cte, nomefile);
			}
			// cte             cto           nverde             idservizio
			//390438370521	390299371062	800066703	70030	2
            
			if (StringUtils.equals(idServizioWlr,"2")) {
				// numTelSconosciuti+=cte+"\n";
				String tmp=nverdeWlr;
				tmp+="-"+cto;
				cto="39"+cte;//aggiungo di nuovo il 39
				cte=nverdeWlr;
				nverdeWlr=tmp;
				
			}
			
			
			if (!utility.numTelClienti.containsKey(cte)) {
				// numTelSconosciuti+=cte+"\n";
				numTelScon.put(cte, nomefile);
			}
			
			String tipoCh_paese = utility.getTipoChiamata_WLR_VOIP(cte, cto);
			String tc=StringUtils.substringBefore(tipoCh_paese, "_");
			String paese=(StringUtils.substringAfter(tipoCh_paese, "_")).replaceAll("'", "''");
		
			
			
			if(StringUtils.equals(tc, "UNKNOW")) {
				prefissiScon.put(cto, nomefile);
			}

			sql = "INSERT INTO chiamateBELLNET (idfile, telcli, teldestinazione, tipoChiamata, paese, dataora, durata, idservizioWLR, nverdeWLR)"
					+ " VALUES(" + idfile + ",'" + cte + "','" + cto + "','" + tc + "','" + paese + "','" + ts + "'," + durata +",'"
					+idServizioWlr + "','"+nverdeWlr+"');";
			


		} catch (Exception e) {
			logger.error("exception e:" + e.getMessage());
			sql = null;
		}

		return sql;

	}
}