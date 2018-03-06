package com.px.csv;

import com.csvreader.CsvReader;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;

public class CsvTest {

    @Test
    public void CsvReader(){
        try {
            CsvReader csvReader = new CsvReader(new FileInputStream(new File("C:\\Users\\hyt\\Desktop\\test.csv")), ',', Charset.forName("GBK"));
            boolean isFirstLine = true;
            csvReader.getRawRecord();
            while (csvReader.readRecord()){
                String[] values = csvReader.getValues();
                if (isFirstLine){
                    isFirstLine = false;
                }else {
                    for (String value : values){
                        System.out.print(value + " ");
                    }
                    System.out.println();
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
