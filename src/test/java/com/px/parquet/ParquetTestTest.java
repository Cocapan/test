package com.px.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class ParquetTestTest extends ParquetTest {

    private Configuration configuration;
    private Path parquetFilePath;

    @Before
    public void setUp() throws Exception {
        configuration = new Configuration(true);
        configuration.set("fs.defaultFS","hdfs://172.16.13.246:9000");
        parquetFilePath = new Path("/tmp/document.parquet");
    }

    @Test
    public void writeParquet() throws IOException {
        MessageType schema = Types.buildMessage()
                .required(PrimitiveType.PrimitiveTypeName.INT32).named("DocId")
                .optionalGroup()
                .repeated(PrimitiveType.PrimitiveTypeName.INT32).named("Backward")
                .repeated(PrimitiveType.PrimitiveTypeName.INT32).named("Forward")
                .named("Links")
                .repeatedGroup()
                .repeatedGroup()
                .required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("Code")
                .optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("Country")
                .named("Language")
                .optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named("Url")
                .named("Name")
                .named("Document");
//        System.out.println(schema.toString());
        GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        groupWriteSupport.setSchema(schema,configuration);
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(parquetFilePath)){
            fileSystem.delete(parquetFilePath,true);
        }
        fileSystem.close();
        ParquetWriter<Group> writer = new ParquetWriter<Group>(parquetFilePath,configuration,groupWriteSupport);
        Group document1 = groupFactory.newGroup().append("DocId", 1);
        document1.addGroup("Links").append("Backward",10).append("Forward",20);
        Group name = document1.addGroup("Name");
        name.addGroup("Language").append("Code","UTF8").append("Country","China");
        name.append("Url","www.baidu.com");

        Group document2 = groupFactory.newGroup().append("DocId", 2);
        document2.addGroup("Links").append("Backward",11).append("Forward",21);
        Group name2 = document2.addGroup("Name");
        name2.addGroup("Language").append("Code","en-us").append("Country","America");
        name2.append("Url","www.youku.com");
        writer.write(document1);
        writer.write(document2);
        writer.close();
    }

    @Test
    public void readParquet() throws IOException {
        GroupReadSupport groupReadSupport = new GroupReadSupport();
        ParquetReader.Builder<Group> builder = ParquetReader.builder(groupReadSupport, parquetFilePath);
        builder.withConf(configuration);
        ParquetReader<Group> groupParquetReader = builder.build();
        Group row = null;
        while ((row = groupParquetReader.read()) != null){
            System.out.println(row.toString());
        }
        groupParquetReader.close();
    }

    @Test
    public void getMetaData() throws IOException {
        ParquetMetadata footer = ParquetFileReader.readFooter(configuration, parquetFilePath, ParquetMetadataConverter.NO_FILTER);
        System.out.println("###########################FileMeta##############################");
        FileMetaData fileMetaData = footer.getFileMetaData();
        System.out.println(fileMetaData.toString());
        MessageType schema = fileMetaData.getSchema();
        System.out.println("Schema: " + fileMetaData.getSchema());
        for (Iterator<ColumnDescriptor> iterator = schema.getColumns().iterator(); iterator.hasNext(); ) {
            ColumnDescriptor columnDescriptor = iterator.next();
            System.out.println("Type: " + columnDescriptor.getType());
            System.out.println("TypeLength: " + columnDescriptor.getTypeLength());
            System.out.println("MaxRepetitionLevel: " + columnDescriptor.getMaxRepetitionLevel());
            System.out.println("MaxDefinitionLevel: " + columnDescriptor.getMaxDefinitionLevel());
            String[] columnDescriptorPath = columnDescriptor.getPath();
            for (String path:columnDescriptorPath) {
                System.out.println("Path: " + path);
            }
        }
        System.out.println("CreateBy: " + fileMetaData.getCreatedBy());
        System.out.println("KeyValueMetaData: " + fileMetaData.getKeyValueMetaData());
        System.out.println("#################################################################");
        List<BlockMetaData> blocks = footer.getBlocks();
        for (BlockMetaData bmd:blocks) {
            List<ColumnChunkMetaData> columns = bmd.getColumns();
            System.out.println("*************************BlockMetaData***************************");
            System.out.println(bmd.toString());
            System.out.println("Path: " + bmd.getPath());
            System.out.println("StartingPos: " + bmd.getStartingPos());
            System.out.println("CompressedSize: " + bmd.getCompressedSize());
            System.out.println("RowCount:" + bmd.getRowCount());
            System.out.println("TotalByteSize: " + bmd.getTotalByteSize());
            System.out.println("*****************************************************************");
            for (ColumnChunkMetaData ccmd:columns) {
                System.out.println("-----------------------ColumnChunkMetaData-----------------------");
                System.out.println(ccmd.toString());
                System.out.println("Codec: " + ccmd.getCodec());
                System.out.println("DictionaryPageOffset: " + ccmd.getDictionaryPageOffset());
                System.out.println("Encodings: " + ccmd.getEncodings().toString());
                System.out.println("FirstDataPageOffset: " + ccmd.getFirstDataPageOffset());
                System.out.println("Path: " + ccmd.getPath());
                System.out.println("StartingPos: " + ccmd.getStartingPos());
                System.out.println("Statistics: " + ccmd.getStatistics());
                System.out.println("TotalSize: " + ccmd.getTotalSize());
                System.out.println("TotalUncompressedSize: " + ccmd.getTotalUncompressedSize());
                System.out.println("Type: " + ccmd.getType());
                System.out.println("ValueCount: " + ccmd.getValueCount());
                System.out.println("-----------------------------------------------------------------");
            }
        }
    }

}