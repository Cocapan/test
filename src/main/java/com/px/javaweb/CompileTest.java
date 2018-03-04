package com.px.javaweb;

public class CompileTest {

    public static void main(String[] args) {
        com.sun.tools.javac.main.Main compiler= new com.sun.tools.javac.main.Main("javac");
        compiler.compile(args);
    }

}
