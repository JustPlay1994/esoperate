package com.justplay1994.github.esoperate.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by dongxin on 2018/6/6.
 */
public class InputData {
    private static final Logger logger = LoggerFactory.getLogger(InputData.class);

    public static void main(String[] args) {
        StringBuilder stringBuilder = new StringBuilder();
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                8,
                8,
                100,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(8)     //等待队列
        );

        for (int i = 0; i < OutputData.tables.length; ++i) {
            String tbName = OutputData.tables[i];

            File filePath = new File("in/"+tbName + "1.csv");

            try {
                FileInputStream in = new FileInputStream(filePath);
                InputStreamReader inReader = new InputStreamReader(in, "UTF-8");
                BufferedReader bufReader = new BufferedReader(inReader);
                String line = null;
                int j = 1;
                while((line = bufReader.readLine()) != null){
                    System.out.println("第" + i + "行：" + line);
                    line = line.replace(" ","");
                    String[] strings = line.split(",");
//                    stringBuffer.append("{ \"update\":{ \"_index\": \"db_poi_test@baidupoi_utf_8\", \"_type\": \"_doc\"}}");
                    String id = strings[0];
                    String lon = strings[2];
                    String lat = strings[3];
                    String update = "{\"update\":{ \"_index\": \""+ESOperate.getIndexName(ESOperate.dbName,tbName)+"\", \"_type\": \"_doc\",\"_id\":\""+id+"\"}}\n" +
                            "{\"doc\":{\"location\": {\"lon\": \""+lon+"\",\"lat\": \""+lat+"\"}}}\n";
                    stringBuilder.append(update);
                    if (stringBuilder.length() > 5*1024*1024){
                        executor.execute(new Thread(new MyURLConnectionThread(ESOperate.esURL+"_bulk","POST",stringBuilder.toString())));
                        stringBuilder.delete(0, stringBuilder.length());/*清空*/
                    }
                    /*如果当前线程数达到最大值，则阻塞等待*/
                    while(executor.getQueue().size()>=executor.getMaximumPoolSize()){
                        logger.debug("Thread waite ...Already maxThread. Now Thread nubmer:"+executor.getActiveCount());
//                            logger.debug("线程池中线程数目："+executor.getPoolSize()+"，队列中等待执行的任务数目："+executor.getQueue().size()+"，已执行完别的任务数目："+executor.getCompletedTaskCount());
                        long time = 100;
                        try {
                            Thread.sleep(time);
                        } catch (InterruptedException e) {
                            logger.error("sleep error!",e);
                        }
                    }
                    j++;
                }
                bufReader.close();
                inReader.close();
                in.close();
            } catch (Exception e) {
                logger.error("read file " + filePath + "error！\n", e);
            }
        }
        executor.execute(new Thread(new MyURLConnectionThread(ESOperate.esURL + "_bulk", "POST", stringBuilder.toString())));

        while(executor.getActiveCount()!=0 || executor.getQueue().size()!=0){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.error("sleep error!\n",e);
            }
        }
            /*关闭线程池*/
        executor.shutdown();
    }
}
