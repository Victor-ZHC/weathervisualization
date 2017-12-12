package com.adc.disasterforecast.tools;

import org.apache.poi.ss.usermodel.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExcelHelper {
    private static String basePath = "src/main/java/com/adc/disasterforecast/warningfile";

    public static List<Row> loadAllExcelFile() {
        List<Row> result = new ArrayList<>();
        FileInputStream inStream = null;

        try {
            File file = new File(basePath);

            if (! file.isDirectory()) {
                throw new Exception("file directory did not exist!");
            }

            File[] files = file.listFiles();

            for (int i = 0; i < files.length; i++) {
                String filePath = files[i].getAbsolutePath();
                inStream = new FileInputStream(new File(filePath));
                Workbook workBook = WorkbookFactory.create(inStream);

                Sheet sheet = workBook.getSheetAt(0);

                int rowNum = sheet.getLastRowNum() + 1;

                for(int j = 0; j < rowNum; j++){
                    Row row = sheet.getRow(j);
                    if (row != null) {
                        result.add(row);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            try {
                if(inStream != null){
                    inStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return result;
    }

    public static String getCellContent(Row row, int index) {
        String result = "";

        Cell cell = row.getCell(index);
        DataFormatter formatter = new DataFormatter();
        if (cell != null) {
            //判断单元格数据的类型，不同类型调用不同的方法
            switch (cell.getCellType()) {
                //数值类型
                case Cell.CELL_TYPE_NUMERIC:
                    //进一步判断 ，单元格格式是日期格式
                    if (DateUtil.isCellDateFormatted(cell)) {
                        result = formatter.formatCellValue(cell);
                    } else {
                        //数值
                        double value = cell.getNumericCellValue();
                        int intValue = (int) value;
                        result = value - intValue == 0 ? String.valueOf(intValue) : String.valueOf(value);
                    }
                    break;
                case Cell.CELL_TYPE_STRING:
                    result = cell.getStringCellValue();
                    break;
                case Cell.CELL_TYPE_BOOLEAN:
                    result = String.valueOf(cell.getBooleanCellValue());
                    break;
                case Cell.CELL_TYPE_BLANK:
                    result = "";
                    break;
                case Cell.CELL_TYPE_ERROR:
                    result = "";
                    break;
                default:
                    result = cell.toString().trim();
                    break;
            }
        }

        return result.trim();
    }

    public static int getWarningYear(String content) {
        String[] time = content.split(" ");
        String[] date = time[0].split("-");
        return Integer.valueOf(date[0]);
    }

    public static String getWarningDate(String content) {
        String[] time = content.split(" ");
        return time[0] + "T" + time[1];
    }

    public static String getWarningType(String content) {
        if (content.contains("雷电")) {
            return "雷电";
        } else if (content.contains("大风")) {
            return "大风";
        } else if (content.contains("暴雨")) {
            return "暴雨";
        } else {
            return "";
        }
    }
}
