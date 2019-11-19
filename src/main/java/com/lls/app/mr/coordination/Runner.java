package com.lls.app.mr.coordination;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Map;

/************************************
 * Runner
 * 基于物品的协同过滤算法实现
 * @author liliangshan
 * @date 2019/11/19
 ************************************/
public interface Runner {

    boolean run(Configuration config, Map<String, String> map)
            throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException;

}
