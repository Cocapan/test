package com.px.hbase;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ImportTest {

    public static class CsvImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Writable>{

        public static String[] column;
        public static String[] qulifier;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            if (key.equals(0)){
                column = new String[values.length];

            }
        }

    }

}
