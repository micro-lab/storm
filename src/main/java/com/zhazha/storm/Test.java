package com.zhazha.storm;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Test {
    public static void main(String[] args){
        Map<String,String> map = new HashMap<String,String>();
        map.put("kaige","dakaige");
        String name = map.get("kaige");
        System.out.println(name==null);
    }
}
