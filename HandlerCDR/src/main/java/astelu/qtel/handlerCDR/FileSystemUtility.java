package astelu.qtel.handlerCDR;

import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class FileSystemUtility {
	final static Logger logger = Logger.getLogger(FileSystemUtility.class.getName());
	public static void main(String[] args) {

		// TODO Auto-generated method stub
		//String pathdir = "/home/luigi/Documenti/work/astelutilities/FileDaAdattare/";

	}

	/**
	 * 
	 * @param dirPath
	 * @param estensione
	 * @return
	 */
	public static TreeMap<String, String> getFileList(String dirPath,String estensione) {
		DirectoryStream<Path> stream = null;
		TreeMap<String, String> tmap = new TreeMap<String, String>();
		try {
			Path dir = FileSystems.getDefault().getPath(dirPath);
			// Path dir =
			// FileSystems.getDefault().getPath("/tmp/2015/04-aprile");
			stream = Files.newDirectoryStream(dir);

			for (Path path : stream) {
				//System.out.println( path.getFileName()+" - "+(++i) );
				logger.debug("fileName:"+path.getFileName());
				if (path.getFileName().toString().endsWith(estensione)) // è file cdr
					tmap.put(path.getFileName().toString(), path.getFileName().toString());
			}
			stream.close();
			// fileNameOrdered(tmap);

		} catch (Exception e) {
			try {
				stream.close();
				logger.error("exception:"+e.getMessage());
			} catch (Exception e2) {
				logger.error("exception:"+e2.getMessage());
			}

		}

		return tmap;

	}
	
	/**
	 * 
	 * @param dirPath
	 * @param estensione
	 * @return
	 */
	public static TreeMap<String, String> getFileListMonth(String dirPath,String estensione) {
		DirectoryStream<Path> stream = null;
		TreeMap<String, String> tmap = new TreeMap<String, String>();
		try {
			Path dir = FileSystems.getDefault().getPath(dirPath);
			// Path dir =
			// FileSystems.getDefault().getPath("/tmp/2015/04-aprile");
			stream = Files.newDirectoryStream(dir);

			for (Path path : stream) {
				//System.out.println( path.getFileName()+" - "+(++i) );
				logger.debug("fileName:"+path.getFileName());
				if (path.getFileName().toString().endsWith(estensione)) // è file cdr
					tmap.put(path.getFileName().toString(), path.getFileName().toString());
			}
			stream.close();
			// fileNameOrdered(tmap);
			/*
			for (int a = 0; a < sf.length; a++) {
				if (sf[a].getName().endsWith(".txt")) {
					StringTokenizer st = new StringTokenizer(sf[a].getName(), "_");
					st.nextToken();
					String md = st.nextToken();
					String month=md.substring(0,3).toUpperCase();
					String currentMonth=StringUtils.substring(LocalDate.now().getMonth().name(),0,3);
					
					if(currentMonth.equals(month))
						tm.put(sf[a].getName(), Long.parseLong(StringUtils.substring(md, 3)));
				}
			}
			*/

		} catch (Exception e) {
			try {
				stream.close();
				logger.error("exception:"+e.getMessage());
			} catch (Exception e2) {
				logger.error("exception:"+e2.getMessage());
			}

		}

		return tmap;

	}
	
	

	/**
	 * 
	 * @param dirPath
	 * @return
	 */
	public static TreeMap<String, Long> getFileListL(String dirPath) {
		DirectoryStream<Path> stream = null;
		TreeMap<String, Long> tmap = new TreeMap<String, Long>();
		try {
			Path dir = FileSystems.getDefault().getPath(dirPath);
			// Path dir =
			// FileSystems.getDefault().getPath("/tmp/2015/04-aprile");
			stream = Files.newDirectoryStream(dir);

			for (Path path : stream) {
				//System.out.println( path.getFileName()+" - "+(++i) );
				logger.debug("fileName:"+path.getFileName());
				if (path.getFileName().toString().endsWith(".cdr")) // è file cdr
					tmap.put(path.getFileName().toString(), Long.parseLong(StringUtils.substringBefore(path.getFileName().toString(), ".cdr")));
			}
			stream.close();
			// fileNameOrdered(tmap);

		} catch (Exception e) {
			try {
				stream.close();
				logger.error("exception:"+e.getMessage());
			} catch (Exception e2) {
				logger.error("exception:"+e2.getMessage());
			}

		}

		return tmap;

	}
	
	public static String getLastFile(String dirPath,String estensione) {
		TreeMap<String, String> tm=getFileList(dirPath,estensione);
		if(tm.isEmpty()) 
			return null;
		return tm.lastKey().toString();
	}
	
		
	public static void printFileNameOrdered(TreeMap<String,String> t) {

		/* Display content using Iterator */
		Set<Entry<String, String>> set = t.entrySet();
		Iterator<Entry<String, String>> iterator = set.iterator();
		while (iterator.hasNext()) {
			@SuppressWarnings("rawtypes")
			Map.Entry mentry = (Map.Entry) iterator.next();
			// System.out.print("key is: "+ mentry.getKey() + " & Value is: ");
			System.out.println(mentry.getValue());
		}

	}

}
