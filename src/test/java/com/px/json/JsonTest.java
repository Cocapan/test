package com.px.json;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.apache.storm.hdfs.spout.Configs;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.*;

/**
 * @Authon:panxuan
 * @Description:
 * @Date: Created in 11:00 2018/1/3
 * @Modified By:
 */
public class JsonTest {

    private enum DataSourceType{
        HDFS,KAFKA
    }

    @Test
    public  void parseString() {
        JsonParser jsonParser = new JsonParser();
        JsonElement element = jsonParser.parse("{'fields':'word'}");
        String fields = element.getAsJsonObject().get("fields").getAsString();
        System.out.println(fields);
//        String dataSource = "{'hdfs':{'hdfsUri':'hdfs://master:9000','sourceDir':'/testData','archiveDir':'/stormArchive','badFilesDir':'/stormBadFiles','readerType':'text','outputFields':'line'}}";
//        Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
//        Type type = new TypeToken<Map<String, Map<String, String>>>() {
//        }.getType();
//        Map<String, Map<String, String>> dataSourceMap = gson.fromJson(dataSource,type);
//        String key = dataSourceMap.keySet().iterator().next();
//        DataSourceType dataSourceType = DataSourceType.valueOf(key.toUpperCase());
//        switch (dataSourceType){
//            case HDFS:
//        }
    }

    private enum HdfsConfig{
        fsUrl,path,fieldDelimiter,fileSize,countSyncPolicy
    }

    @Test
    public void parseJsonByObject(){
        String dataSource = "{'kafka':{'zkHosts':'172.16.13.249:2181,172.16.13.250:2181,172.16.13.251:2181','topicName':'stormtest','zkRoot':'/stormKafka/stormtest','schemeType':'string'}}";
        Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
        HashMap<String, KafkaConfig> hashMap = gson.fromJson(dataSource, new HashMap<String, KafkaConfig>().getClass());
        System.out.println(hashMap.keySet().iterator().next());
        System.out.println(hashMap.values().iterator().next());
    }

    @Test
    public void enumTest(){
        for (HdfsConfig hdfsConfig : HdfsConfig.values()) {
            System.out.println(hdfsConfig.name());
        }
    }
    @Test
    public void generateJson(){
        Map<String,Map<String,String>> map = new HashMap<String,Map<String,String>>();
        Map<String,String> map1 = new HashMap<>();
        map1.put("a","a");
        map1.put("b","b");
        map.put("key1", map1);

        Map<String,Map<String,String>> map2 = new HashMap<String,Map<String,String>>();

        map2.put("key2", map1);

        List<Map<String,Map<String,String>>> list = new ArrayList<Map<String,Map<String,String>>>();
        list.add(map);
        list.add(map2);

        Gson gson =  new Gson();

        String jsonString = gson.toJson(list);

        System.out.println("json字符串:"+jsonString);
    }

    @Test
    public void parseTopology(){
        String jobTopology = "[{'HdfsSpout':{'last':'','next':'SplitBolt'}},{'SplitBolt':{'last':'HdfsSpout','next':'CountBolt'}},{'HdfsBolt':{'last':'CountBolt','next':''}}]";
        Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
        Type type = new TypeToken<List<Map<String, Map<String, String>>>>() {
        }.getType();
        List<Map<String, Map<String, String>>> orderList = gson.fromJson(jobTopology,type);
        System.out.println(orderList.size());
        for (int i=0;i<orderList.size();i++){
            Map<String, Map<String, String>> firstComponentMap = orderList.get(i);
            String componentName = firstComponentMap.keySet().iterator().next();
            Map<String, String> lastAndNextMap = firstComponentMap.get(componentName);
            String last = lastAndNextMap.get("last");
            String next = lastAndNextMap.get("next");
            if (last == null || last.equals("")){
                System.out.println(componentName);
            }
            if (next != null && !next.equals("")){
                System.out.println(next);
            }else {
                System.out.println(componentName);
            }
        }

//        for (int i=0;i<orderList.size();i++){
//            Map<String, Map<String, String>> firstComponentMap = orderList.get(i);
//            String componentName = firstComponentMap.keySet().iterator().next();
//            Map<String, String> beforeAndNextMap = firstComponentMap.get(componentName);
//            String before = beforeAndNextMap.get("before");
//            String next = beforeAndNextMap.get("next");
//            if (before == null || before.equals("")){
//                System.out.println(componentName);
//            }
//            if (next != null && !next.equals("")){
//                System.out.println(next);
//            }
//        }
    }

    @Test
    public void parseTopology1(){
        String jobTopology = "{'HdfsSpout':'aa','SplitBolt':'bb','HdfsBolt':'cc'}";
        Gson gson = new GsonBuilder().enableComplexMapKeySerialization().create();
        Type type = new TypeToken<Arrays>() {
        }.getType();
        Arrays orderList = gson.fromJson(jobTopology,type);

    }

    @Test
    public void parseTopologyByObject(){

        String jobTopology = "{'HdfsSpout':{'lastComponent':'','nextComponent':'SplitBolt'}}";
        Gson gson = new Gson();
        ComponentChain componentChain = gson.fromJson(jobTopology, ComponentChain.class);
        System.out.println(componentChain);

    }





}
