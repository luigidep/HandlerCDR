package astelu.qtel.handlerCDR;

import java.io.File;
import java.io.FileInputStream;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ReadExcel2013 {

	public static void main(String[] args) {

		try {

			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
			File myFile = new File("/home/luigi/Documenti/work/astelutilities/Archivio Clienti.xlsx");
			FileInputStream fis = new FileInputStream(myFile);

			XSSFWorkbook myWorkBook = new XSSFWorkbook(fis);
			System.out.println("new XSSFWorkbook");

			XSSFSheet mySheet = myWorkBook.getSheet("Wlr");

			Iterator<Row> rowIterator = mySheet.iterator(); // Traversing over

			while (rowIterator.hasNext()) {
				Row row = rowIterator.next();

				Iterator<Cell> cellIterator = row.cellIterator();

				int i = 0;
				while (cellIterator.hasNext()) {

					Cell cell = cellIterator.next();
					if (i == 0 && StringUtils.isEmpty(cell.toString()))
						break;
					switch (cell.getCellType()) {
					case Cell.CELL_TYPE_STRING:
						String s = cell.getStringCellValue().replaceAll("(\\r|\\n)", " ").trim();
						System.out.print("s:" + s + "\t");
						break;
					case Cell.CELL_TYPE_NUMERIC:
						if (DateUtil.isCellDateFormatted(cell)) {
							Date date = cell.getDateCellValue();
							LocalDate ldate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
							String sdate = ldate.format(formatter);
							System.out.print("date:" + sdate + "\t");

						} else {

							Double d = cell.getNumericCellValue();
							System.out.print("n:" + d + "\t");
						}

						break;
					case Cell.CELL_TYPE_BOOLEAN:
						System.out.print("b:" + cell.getBooleanCellValue() + "\t");
						break;
					case Cell.CELL_TYPE_BLANK:
						System.out.print("b:" + cell.getStringCellValue() + "\t");
					default:
						System.out.print("d:" + cell.getStringCellValue() + "\t");
					}
					++i;
				}

				System.out.println("i=" + i);
			}
			myWorkBook.close();
			fis.close();
		} catch (Exception e) {
			e.printStackTrace();

		}

	}

}
