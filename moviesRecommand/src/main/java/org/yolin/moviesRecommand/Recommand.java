package org.yolin.moviesRecommand;


import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class Recommand {
    public static final String HDFS="hdfs://127.0.0.1:9000";
    public static final Pattern DELIMITER=Pattern.compile("[\t,]");
    private static String homeDir="/home/yolin/gitworkspace/hadoop_ProjectTest/moviesRecommand/";
    private static Map<String,String> setPath(){
        Map<String,String> path=new HashMap<>();
        path.put("data",homeDir+"data/small.csv");
        path.put("step1Input",HDFS+"/recommand");
        path.put("step1Output",path.get("step1Input")+"/step1");
        path.put("step2Input",path.get("step1Output"));
        path.put("step2Output",path.get("step1Input")+"/step2");
        path.put("step3Input1",path.get("step1Output"));
        path.put("step3Output1",path.get("step1Input")+"/step3_1");
        path.put("step3Input2",path.get("step2Output"));
        path.put("step3Output2",path.get("step1Input")+"/step3_2");
        path.put("step4Input1",path.get("step3Output1"));
        path.put("step4Input2",path.get("step3Output2"));
        path.put("step4Output",path.get("step1Input")+"/step4");
         return path;
        }
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException, SQLException {
        Map<String,String> path=setPath();

        //按用户记录itemid和评分,input value=userid itemid pref
        //output key=userid,value=itemid1:pref1,itemid2:pref2...
        step1.run(path);
        //input是step1的output（按用户分组）key=userid,value=itemid1:pref1,itemid2:pref2...
        // output是被同一个用户评分的两个item的组合,key=item1:item2,value=concurr times
        step2.run(path);
        //input是step1的output（按用户分组(key=userid,value=itemid1:pref1,itemid2:pref2,...)）,
        // output是对于每一个item，用户和他给的评分(itemid,userid:pref)
        //key=itemid,value=userid:pref
        step3.run1(path);
        //BasicConfigurator.configure();
        //input是被同一个用户评分的两个item的组合,key=item1:item2,value=concurr times
        //output is the same as step2
        step3.run2(path);

        step4.run(path);
        //输出结果总是空的，可能需要用到才生成
        //结果输出到mysql
        String output=path.get("step4Output")+"/part-r-00000";
        Configuration conf=new Configuration();
        org.apache.hadoop.fs.FileSystem fs;
        fs= org.apache.hadoop.fs.FileSystem.get(URI.create(output),conf);

        InputStream in=fs.open(new Path(output));
        //IOUtils.copy(in,System.out);
        System.out.println(IOUtils.toString(in,"utf-8"));
        //step5.run();



        System.exit(0);
    }
    public static Job config()throws IOException{
        org.apache.hadoop.conf.Configuration conf=new org.apache.hadoop.conf.Configuration();
         Job job=Job.getInstance(conf,"RecommandJob");
         job.setJarByClass(Recommand.class);
        return job;
    }
}
