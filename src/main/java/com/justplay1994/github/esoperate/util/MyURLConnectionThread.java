package com.justplay1994.github.esoperate.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;

/**
 * 客户端
 */
public class MyURLConnectionThread implements Runnable{
    private static final Logger logger = LoggerFactory.getLogger(MyURLConnectionThread.class);
    private String url;
    private String type;
    private String body;
    private String result;

    public MyURLConnectionThread(String url, String type, String body){
        this.url = url;
        this.type = type;
        this.body = body;
    }

    @Override
    public void run() {
        URLConnection urlConnection = null;
        HttpURLConnection httpURLConnection=null;
        try {
            logger.debug("[ur: " + url + " ,type: " + type + " ,body: ]\n" + body);
//            URL url = new URL(url);
            this.url = url;
            this.type = type;
            this.body = body;

            urlConnection = new URL(url).openConnection();
            httpURLConnection = (HttpURLConnection) urlConnection;

            /*输入默认为false，post需要打开*/
            httpURLConnection.setDoInput(true);
            httpURLConnection.setDoOutput(true);
            httpURLConnection.setRequestProperty("Content-Type", "application/json");

            httpURLConnection.setRequestMethod(type);


//            httpURLConnection.setConnectTimeout(3000);


            httpURLConnection.connect();


            OutputStream outputStream = httpURLConnection.getOutputStream();

            outputStream.write(body.getBytes());


            InputStream inputStream = httpURLConnection.getInputStream();


            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"));
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line).append("\n");
            }
            result = builder.toString();
            logger.debug(result);

        } catch (MalformedURLException e) {
            logger.error("URL",e);
        } catch (UnsupportedEncodingException e) {
            logger.error("error",e);
        } catch (ProtocolException e) {
            logger.error("error", e);
        } catch (IOException e) {
            logger.error("error", e);
        } finally{
            assert httpURLConnection != null;
            httpURLConnection.disconnect();/*关闭连接*/
        }
    }
}
