package com.px.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.mapreduce.ImportTsv;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TsvImporterMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Pattern;

public class HbaseTest {

    private Connection connection;
    private Admin admin;
    private Table table;

    @Before
    public void getConnection(){
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "panxuan-two,panxuan-three");
        UserGroupInformation.setConfiguration(configuration);
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getTableListByDatabase(){
        try {
            admin = connection.getAdmin();
            TableName[] tableNames = admin.listTableNamesByNamespace("test");
            for (TableName tableName : tableNames){
                System.out.println(tableName.getNameAsString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getTableListByPattern(){
        try {
            admin = connection.getAdmin();
            TableName[] tableNames = admin.listTableNames(Pattern.compile("test:.*"));
            for (TableName tableName : tableNames){
                System.out.println(tableName.getNameAsString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteTable(){
        try {
            admin = connection.getAdmin();
            admin.disableTable(TableName.valueOf("test:laijian"));
            admin.deleteTable(TableName.valueOf("test:laijian"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteTables(){
        try {
            admin = connection.getAdmin();
            admin.disableTables("test:lai.*");
            admin.deleteTables("test:lai.*");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createTable(){
        try {
            String namespace = "test";
            boolean isNamespaceExist = false;
            admin = connection.getAdmin();
            NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
            for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors){
                if (namespaceDescriptor.getName().equals(namespace)){
                    isNamespaceExist = true;
                }
            }
            if (!isNamespaceExist){
                admin.createNamespace(NamespaceDescriptor.create(namespace).build());
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("test:panxuan"));
            HColumnDescriptor ageColumnDescriptor = new HColumnDescriptor("age");
            HColumnDescriptor addressColumnDescriptor = new HColumnDescriptor("address");
            ageColumnDescriptor.setMaxVersions(5);
            ageColumnDescriptor.setCompressionType(Compression.Algorithm.GZ);
            ageColumnDescriptor.setTimeToLive(3000000);
            ageColumnDescriptor.setBlockCacheEnabled(false);
            tableDescriptor.addFamily(ageColumnDescriptor);
            tableDescriptor.addFamily(addressColumnDescriptor);
            tableDescriptor.setRegionReplication(1);
            admin.createTable(tableDescriptor,new RegionSplitter.HexStringSplit().split(1));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void updateTable(){
        try {
            admin = connection.getAdmin();
            TableName tableName = TableName.valueOf("test:panxuan");
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
            HColumnDescriptor address = tableDescriptor.getFamily("age".getBytes());
            address.setMaxVersions(3);
            address.setCompressionType(Compression.Algorithm.GZ);
            address.setTimeToLive(3000);
            address.setBlockCacheEnabled(false);
            tableDescriptor.setRegionReplication(2);
            if (admin.isTableEnabled(tableName)){
                admin.disableTable(tableName);
                admin.modifyColumn(tableName,address);
                admin.modifyTable(tableName,tableDescriptor);
            }
            admin.enableTable(tableName);
//            admin.flush(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void queryTable(){
        try {
            admin = connection.getAdmin();
            ArrayList<String> tableList = new ArrayList<>();
            tableList.add("test:laijian");
            tableList.add("test:panxuan");
            StringBuilder sb = new StringBuilder();
            int tableCount = 0;
            for (String tableName : tableList){
                if (tableCount == 0){
                    sb.append("[{\"").append(tableName).append("\":[{");
                }else {
                    sb.append(",{\"").append(tableName).append("\":[{");
                }
                HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
                Collection<HColumnDescriptor> families = tableDescriptor.getFamilies();
                int columnCount = 0;
                for (HColumnDescriptor hColumnDescriptor : families){
                    if (columnCount == 0){
                        sb.append("\"name\":\"").append(hColumnDescriptor.getNameAsString()).append("\",\"Version\":\"")
                                .append(hColumnDescriptor.getMaxVersions()).append("\",\"Compression\":\"").append(hColumnDescriptor.getCompressionType().getName())
                                .append("\",\"TimeToLive\":\"").append(hColumnDescriptor.getTimeToLive()).append("\",\"BlockCache\":\"").append(hColumnDescriptor.isBlockCacheEnabled()).append("\"}");
                    }else {
                        sb.append(",{\"name\":\"").append(hColumnDescriptor.getNameAsString()).append("\",\"Version\":\"")
                                .append(hColumnDescriptor.getMaxVersions()).append("\",\"Compression\":\"").append(hColumnDescriptor.getCompressionType().getName())
                                .append("\",\"TimeToLive\":\"").append(hColumnDescriptor.getTimeToLive()).append("\",\"BlockCache\":\"").append(hColumnDescriptor.isBlockCacheEnabled()).append("\"}");
                    }
                    if (columnCount == families.size()-1){
                        sb.append("]}");
                    }
                    columnCount++;
                }
                if (tableCount == tableList.size()-1){
                    sb.append("]");
                }
                tableCount ++;
            }
            System.out.println(sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void tableExists(){
        try {
            admin = connection.getAdmin();
            String tableName = "test:laijian";
            if (admin.tableExists(TableName.valueOf(tableName))){
                System.out.println("table " + tableName + " exists!");
            }else {
                System.out.println("table " + tableName + " not exists!");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void dataInsert(){
        String tableName = "panxuan";
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes("1"));
            put.addColumn("age".getBytes(),"birthday".getBytes(),Bytes.toBytes("1995-09-02"));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void query(){
        String tableName = "panxuan";
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes("1"));
            scan.setStopRow(Bytes.toBytes("10"));
            scan.setMaxVersions(10);
            scan.setTimeRange(0,Long.MAX_VALUE);
//            scan.addFamily("age".getBytes());
            scan.addColumn("age".getBytes(),"birthday".getBytes());
            ResultScanner results = table.getScanner(scan);
            for(Result result : results){
                for (Cell cell : result.listCells()){
                    getCellInfo(cell);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void queryByRange(){
        String tableName = "panxuan";
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes("age"), Bytes.toBytes("birthday"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("1995-09-02"));
            scan.setFilter(singleColumnValueFilter);
            ResultScanner results = table.getScanner(scan);
            for(Result result : results){
                for (Cell cell : result.listCells()){
                    getCellInfo(cell);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void queryByRowKey(){
        String tableName = "panxuan";
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes("1"));
            Result result = table.get(get);
            for (Cell cell : result.listCells()){
                getCellInfo(cell);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void importData(){
        String table = "importTable";
        String sourceFilePath = "C:\\Users\\hyt\\Desktop\\im.csv";
        Configuration configuration = connection.getConfiguration();
        try {
            Job csvImportJob = Job.getInstance(configuration, "CsvImportTest");
            csvImportJob.setJarByClass(ImportTest.class);
            FileInputFormat.addInputPath(csvImportJob,new Path(sourceFilePath));
            csvImportJob.setMapperClass(ImportTest.CsvImportMapper.class);
            csvImportJob.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,table);
            csvImportJob.setOutputKeyClass(ImmutableBytesWritable.class);
            csvImportJob.setOutputValueClass(Writable.class);
            csvImportJob.setNumReduceTasks(0);
            System.exit(csvImportJob.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void getCellInfo(Cell cell){
        System.out.println(cell.getTimestamp());
        System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
        System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
        System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
        System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
    }

    @After
    public void releaseResource(){
        try {
            if (admin != null){
                admin.close();
            }
            if (table != null){
                table.close();
            }
            if (connection != null){
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
