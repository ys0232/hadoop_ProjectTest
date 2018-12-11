package org.yolin.kpiTest;

import java.util.stream.StreamSupport;

public class test {
    public static void main(String[] args){
        String st="http://,kooskdopsdshttps://";
        st=st.replace("https://","");
        System.out.println(st);
        st=st.replace("http://","");
        System.out.println(st);
    }
}
