package excel;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRichTextString;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.usermodel.HSSFRow;

import shared.ResultFileInfo;

/**
 * A simple POI example of opening an Excel spreadsheet
 * and writing its contents to the command line.
 * @author  Tony Sintes
 */
public class OperationExcel {

    public static void main(String[] args) {
        //String fileName = "C:\\testPOIWrite.xls";
        //writeDataToExcelFile(fileName);
        //readDataToExcelFile(fileName);
    }

    public void readDataToExcelFile(String fileName){
        try{
            FileInputStream fis = new FileInputStream(fileName);
            HSSFWorkbook workbook = new HSSFWorkbook(fis);
            HSSFSheet sheet = workbook.getSheetAt(0);

            for (int rowNum = 0; rowNum < 10; rowNum++) {
                for (int cellNum = 0; cellNum < 5; cellNum++) {
                    HSSFCell cell = sheet.getRow(rowNum).getCell(cellNum);
                    System.out.println(rowNum+":"+cellNum+" = " + cell.getStringCellValue());
                }
            }
            
            fis.close();
        }catch(Exception e){
            e.printStackTrace();
        }


    }
    public void writeDataToExcelFile(String fileName) {
        try {
            HSSFWorkbook myWorkBook = new HSSFWorkbook();
            HSSFSheet mySheet = myWorkBook.createSheet();
            HSSFRow myRow;
            HSSFCell myCell;

            for (int rowNum = 0; rowNum < 10; rowNum++) {
                myRow = mySheet.createRow(rowNum);
                for (int cellNum = 0; cellNum < 5; cellNum++) {
                    myCell = myRow.createCell(cellNum);
                    myCell.setCellValue(new HSSFRichTextString(rowNum + "," + cellNum));
                }
            }
            FileOutputStream out = new FileOutputStream(fileName);
            myWorkBook.write(out);
            out.flush();
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    public void writeDataToExcelFile(String fileName, TreeMap<String, String> resultMap) {
        try {

            HSSFWorkbook myWorkBook = new HSSFWorkbook();
            HSSFSheet mySheet = myWorkBook.createSheet();
            HSSFRow myRow;
            HSSFCell myCell;

            Iterator<Entry<String, String>> iter = resultMap.entrySet().iterator();
            int size = resultMap.size();
            System.out.println("size:"+size);
            int rowNum = 0;
            //for (int rowNum = 0; rowNum < size; rowNum++) {
            while(iter.hasNext()){
                Entry<String, String> entry = iter.next();
                myRow = mySheet.createRow(rowNum);
                myCell = myRow.createCell(0);
                myCell.setCellValue(new HSSFRichTextString(entry.getKey()));
                myCell = myRow.createCell(1);
                myCell.setCellValue(new HSSFRichTextString(entry.getValue()));
                rowNum++;
            }
            
            FileOutputStream out = new FileOutputStream(fileName);
            myWorkBook.write(out);
            out.flush();
            out.close();
            

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    
    public void writeOneBFSMRToExcelFile(String fileName,TreeMap<Integer, TreeMap<TreeSet<Integer>, ResultFileInfo>> storeMap, int iniRow, int iniColumn){
        try {

            HSSFWorkbook myWorkBook = new HSSFWorkbook();
            HSSFSheet mySheet = myWorkBook.createSheet();
            HSSFRow myRow;
            HSSFCell myCell;
            
            
            int originalRow = iniRow;
            
            Iterator<Entry<Integer, TreeMap<TreeSet<Integer>, ResultFileInfo>>> iterStore = storeMap.entrySet().iterator();
            while(iterStore.hasNext()){

                int rowNum = iniRow;
                int columnNum = iniColumn;
                
            	Entry<Integer, TreeMap<TreeSet<Integer>, ResultFileInfo>> entry = iterStore.next();
            	TreeMap<TreeSet<Integer>, ResultFileInfo> resultMap = entry.getValue();
            	Iterator<Entry<TreeSet<Integer>, ResultFileInfo>> iterResult = resultMap.entrySet().iterator();
            	int totalR = 0;
            	while(iterResult.hasNext()){

                    double totalSum = 0;
                    double totalTime = 0;
                    
            		Entry<TreeSet<Integer>, ResultFileInfo> entryResult = iterResult.next();
            		ResultFileInfo resultClass = entryResult.getValue();
            		
            		int sumSize = resultClass.sumList.size();
            		int timeSize = resultClass.runTimeList.size();
            		if(sumSize > timeSize){
            			totalR = sumSize;
            		}
            		else{
            			totalR = timeSize;
            		}
            		////////////////////////////////////////////////////////////////
            		rowNum = iniRow+2;
                    
                    Iterator<Double> iterSum = resultClass.sumList.iterator();
                    while(iterSum.hasNext()){
                        double sum = iterSum.next();
                        myRow = mySheet.getRow(rowNum);
                    	if(myRow==null){
                        	myRow = mySheet.createRow(rowNum);
                    	}
                        myCell = myRow.createCell(columnNum+1);
                        myCell.setCellValue(new HSSFRichTextString(sum+""));
                        totalSum += sum;
                        rowNum++;
                    }
                    
                    rowNum = iniRow + 2;
                    Iterator<Double> iterTime = resultClass.runTimeList.iterator();
                    while(iterTime.hasNext()){
                    	double time = iterTime.next();
                    	myRow = mySheet.getRow(rowNum);
                    	if(myRow==null){
                        	myRow = mySheet.createRow(rowNum);
                    	}
                        myCell = myRow.createCell(columnNum+2);
                        myCell.setCellValue(new HSSFRichTextString(time+""));
                        totalTime += time;
                        rowNum++;
                    }
                    
                    rowNum = iniRow;
                    //write query string
                    myRow = mySheet.getRow(rowNum);
                	if(myRow==null){
                    	myRow = mySheet.createRow(rowNum);
                	}
                    myCell = myRow.createCell(columnNum);
                    myCell.setCellValue(new HSSFRichTextString(resultClass.queryStr));
                    myCell = myRow.createCell(columnNum+1);
                    myCell.setCellValue(new HSSFRichTextString("Sum"));
                    myCell = myRow.createCell(columnNum+2);
                    myCell.setCellValue(new HSSFRichTextString("Time(ms)"));
                    
                    rowNum++;
                    myRow = mySheet.getRow(rowNum);
                	if(myRow==null){
                    	myRow = mySheet.createRow(rowNum);
                	}
                    myCell = myRow.createCell(columnNum);
                    myCell.setCellValue(new HSSFRichTextString(resultClass.queryStr));
                    myCell = myRow.createCell(columnNum+1);
                    myCell.setCellValue(new HSSFRichTextString(totalSum+""));
                    myCell = myRow.createCell(columnNum+2);
                    myCell.setCellValue(new HSSFRichTextString(totalTime+""));
                    ////////////////////////////////////////////////////////////////////////

                    iniRow = iniRow + totalR + 3;
            	}
            	iniRow = originalRow;
            	iniColumn = iniColumn+4;
            }
            
            
            
            
            FileOutputStream out = new FileOutputStream(fileName);
            myWorkBook.write(out);
            out.flush();
            out.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    public void writeOneBFSMRToExcelFile(String fileName, String queryStr, List<Double> sumList, List<Double> runTimeList, int iniRow, int iniColumn) {
        try {

            HSSFWorkbook myWorkBook = new HSSFWorkbook();
            HSSFSheet mySheet = myWorkBook.createSheet();
            HSSFRow myRow;
            HSSFCell myCell;
            
            double totalSum = 0;
            double totalTime = 0;
            
            int rowNum = iniRow;
            int columnNum = iniColumn;
            
            rowNum = iniRow+2;
            
            Iterator<Double> iterSum = sumList.iterator();
            //for (int rowNum = 0; rowNum < size; rowNum++) {
            while(iterSum.hasNext()){
                double sum = iterSum.next();
                myRow = mySheet.getRow(rowNum);
            	if(myRow==null){
                	myRow = mySheet.createRow(rowNum);
            	}
                myCell = myRow.createCell(columnNum+1);
                myCell.setCellValue(new HSSFRichTextString(sum+""));
                totalSum += sum;
                rowNum++;
            }
            
            rowNum = iniRow + 2;
            Iterator<Double> iterTime = runTimeList.iterator();
            while(iterTime.hasNext()){
            	double time = iterTime.next();
            	myRow = mySheet.getRow(rowNum);
            	if(myRow==null){
                	myRow = mySheet.createRow(rowNum);
            	}
                myCell = myRow.createCell(columnNum+2);
                myCell.setCellValue(new HSSFRichTextString(time+""));
                totalTime += time;
                rowNum++;
            }
            
            //write query string
            myRow = mySheet.getRow(rowNum);
        	if(myRow==null){
            	myRow = mySheet.createRow(rowNum);
        	}
            myCell = myRow.createCell(columnNum);
            myCell.setCellValue(new HSSFRichTextString(queryStr));
            myCell = myRow.createCell(columnNum+1);
            myCell.setCellValue(new HSSFRichTextString("Sum"));
            myCell = myRow.createCell(columnNum+2);
            myCell.setCellValue(new HSSFRichTextString("Time(ms)"));
            
            rowNum++;
            myRow = mySheet.getRow(rowNum);
        	if(myRow==null){
            	myRow = mySheet.createRow(rowNum);
        	}
            myCell = myRow.createCell(columnNum);
            myCell.setCellValue(new HSSFRichTextString(queryStr));
            myCell = myRow.createCell(columnNum+1);
            myCell.setCellValue(new HSSFRichTextString(totalSum+""));
            myCell = myRow.createCell(columnNum+2);
            myCell.setCellValue(new HSSFRichTextString(totalTime+""));
            
            
            FileOutputStream out = new FileOutputStream(fileName);
            myWorkBook.write(out);
            out.flush();
            out.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
}
