package com.dianping.auto.tcrunner.utils;


import com.dianping.auto.tcrunner.TCOptionsProcessor;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: pansy.wang
 * Date: 14-9-16
 * Time: 下午3:44
 * To change this template use File | Settings | File Templates.
 */
public class OutFileUtil {

    private static final Logger logger = LoggerFactory.getLogger(TCOptionsProcessor.class.getName());

    public static Iterable<String> readlines(String outfile) throws IOException {
        final FileReader fr = new FileReader(outfile);
        final BufferedReader br = new BufferedReader(fr);

        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {
                    @Override
                    public boolean hasNext() {
                        return line != null;
                    }

                    @Override
                    public String next() {
                        String retval = line;
                        line = getLine();
                        return retval;
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }

                    String getLine() {
                        String line = null;
                        try {
                            line = br.readLine();
                        } catch (IOException e) {
                            line = null;
                        }
                        return line;
                    }
                    String line = getLine();
                };
            }
        };
    }

    public static Map<ArrayList<String>, ArrayList<Float>> outfileToMap(String file, Integer k) throws IOException {
        Map<ArrayList<String>, ArrayList<Float>> outmap = new HashMap<ArrayList<String>, ArrayList<Float>>();
        for (String line : OutFileUtil.readlines(file)) {
            String[] words = StringUtils.split(line);
            ArrayList<String> key = new ArrayList<String>();
            for (int i=0; i<k;i++) {
                key.add(words[i]);
            }
            ArrayList<Float> values = new ArrayList<Float>();
            for (int j=k; j<words.length;j++) {
                values.add(Float.parseFloat(words[j]));
            }
            if (!outmap.containsKey(key)) {
                outmap.put(key, values);
            } else {
                System.err.println("Duplicate record key!");
                logger.error("Duplicate record key!");
                System.exit(2);
            }
        }
        return outmap;
    }

    public static String outfileToString(String file) throws IOException {
        String content = "";
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String line;
            while((line = reader.readLine()) != null) {
                content += line;
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
            logger.error(e.getMessage());
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                    logger.error(e.getMessage());
                }

            }
        }
        return content;
    }

    public static String outfileToConcatString(String file) throws IOException {
        StringBuffer content = new StringBuffer();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String line;
            while((line = reader.readLine()) != null) {
                content += line + ",";
            }
            int last = content.length;
            content.delete(last, last);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            logger.error(e.getMessage());
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    System.err.println(e.getMessage());
                    logger.error(e.getMessage());
                }

            }
        }
        return content.toString();
    }


    public static void main(String[] args) throws IOException {

        Map<ArrayList<String>, ArrayList<Float>> onlineoutmap = OutFileUtil.outfileToMap("online.out", 1);
        Map<ArrayList<String>, ArrayList<Float>> testoutmap = OutFileUtil.outfileToMap("test.out", 1);

        System.out.println(onlineoutmap);
        System.out.println(testoutmap);

        boolean isEqual = onlineoutmap.equals(testoutmap);
        System.out.println(isEqual);

//        String content1 = OutFileUtil.outfileToString("test.out");
//        String content2 = OutFileUtil.outfileToString("online.out");
//        boolean isEqual = StringUtils.equals(content1,content2);
//        System.out.println(isEqual);
    }
}
