package com.px.javaweb;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

public class CharSetTest {

    public static void main(String[] args) {
        String s = "你好，我是中文小子";
        String charsetName = "utf-8";
        if (Charset.isSupported(charsetName)){
            Charset charset = Charset.forName(charsetName);
            byte[] bytes = s.getBytes(charset);
            System.out.println(new String(bytes,charset));
            ByteBuffer byteBuffer = charset.encode(s);
            CharBuffer charBuffer = charset.decode(byteBuffer);
            System.out.println(charBuffer);
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        ByteBuffer byteBuffer1 = byteBuffer.putChar('c');
        System.out.println(byteBuffer1.getChar(0));
    }

}
