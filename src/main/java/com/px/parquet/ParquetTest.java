package com.px.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.*;
import org.apache.parquet.hadoop.api.DelegatingReadSupport;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.*;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ParquetTest {

    public static void main(String[] args) throws IOException {
    }

    public void readParquet(Configuration configuration,Path parquetFilePath) throws IOException {

    }

    public void writeParquet(Configuration configuration,Path parquetFilePath) throws IOException {

    }

    public void getMetaData(Configuration configuration, Path parquetFilePath) throws IOException {

    }

}
