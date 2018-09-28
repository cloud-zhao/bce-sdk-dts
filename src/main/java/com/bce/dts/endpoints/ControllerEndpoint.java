package com.bce.dts.endpoints;

import java.util.ArrayList;
import java.util.List;

/**
 * class description
 * 
 * @author yushaozai@baidu.com
 * @date 2017-08-10
 * @version 1.0
 */
public class ControllerEndpoint {
    public static List<String> listDns;
    public static String environment;
    
    static {
        listDns = new ArrayList<String>();
        // BCE api host
        // su
        listDns.add("dts.pubserver-1.public.bce.baidu.com");
    }

    public static String getDns() {        
//        return listDns.get((int) Math.round(Math.random() * 3));
        return listDns.get(0);
    }
}
