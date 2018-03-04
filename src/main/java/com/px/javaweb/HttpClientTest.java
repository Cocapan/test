package com.px.javaweb;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.Charset;

public class HttpClientTest {

    public static void main(String[] args) throws IOException {
        CloseableHttpClient client = HttpClientBuilder.create().build();
        CloseableHttpResponse closeableHttpResponse = client.execute(new HttpGet("http://www.baidu.com"));
        Header[] headers = closeableHttpResponse.getAllHeaders();
        for(Header header : headers){
            System.out.println(header);
        }
        HttpEntity entity = closeableHttpResponse.getEntity();
        String s = EntityUtils.toString(entity, Charset.forName("utf-8"));
        System.out.println(s);
    }

}
