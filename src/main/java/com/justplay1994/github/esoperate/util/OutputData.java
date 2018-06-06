package com.justplay1994.github.esoperate.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Created by dongxin on 2018/6/6.
 */
public class OutputData {
    private static final Logger logger = LoggerFactory.getLogger(OutputData.class);
    public static String encoding = "UTF-8";

    static String[] tables = {"QAJJ_INSRECORD_V", "QAJJ_PUCENTP_V", "QAJJ_REPACCIDENT_V", "QAJJ_RESFURNISH_V", "QAJJ_RESSTOCKFURNISH_V",
            "QAJJ_RESWAREHOUSE_V", "QAJJ_EMGGOVGOODS_V", "QAJJ_PUCCHEMICALBUILDINFO_V", "QXFJ_WL_CAIJI_INFO_NEW_V", "QXFJ_UNIT_SX_V"};
    static String[] address = {"ADDRESS","ADDRESS","ADDRESS","ADDRESS","ADDRESS","ADDRESS","ADDRESS","BUILDADDRESS","ADDR","UNIT_ADDR"};

    public static void main(String[] args) {

        for (int i = 0; i < tables.length; ++i){
            String tbName = tables[i].toLowerCase();

            try {
                List<LinkedHashMap> result = ESOperate.queryObjectbyIndex(ESOperate.getIndexName(ESOperate.dbName,tbName));

                File file = new File("out/"+tbName+".txt");
                Writer out = new BufferedWriter(
                        new OutputStreamWriter(
                                new FileOutputStream(file), encoding
                        ));
                for (int j = 0; j < result.size(); ++j) {
                    try {
                        out.write(result.get(j).get("ID")+","+result.get(j).get(address[i])+"\n");
                    }catch (Exception e){
                        logger.error("data get id or address error! "+result.get(j),e);
                    }

                }
                out.close();
            } catch (IOException e) {
                logger.error("query es error! tbName: "+tbName,e);
            }
        }
    }
}
