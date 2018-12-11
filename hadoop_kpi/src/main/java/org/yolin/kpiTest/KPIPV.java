package org.yolin.kpiTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

public class KPIPV {
    public static class KPIPVMapper extends Mapper<Object,Text,Text,IntWritable> {

        private IntWritable one=new IntWritable(1);
        private Text word=new Text();
        @Override
        public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context output)
        throws IOException,InterruptedException{
            KPI kpi=KPI.filterPVs(value.toString());
            if (kpi.isValid()){
                word.set(kpi.getRequest());
                output.write(this.word,one);
            }
        }
    }

        public static class KPIPVReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
            private IntWritable result=new IntWritable();

            @Override
            public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
                    throws IOException,InterruptedException {
                int sum=0;

               for (Iterator val=values.iterator();val.hasNext();){
                    IntWritable tmp=(IntWritable)val.next();
                    sum+=tmp.get();
                }
                result.set(sum);
                context.write(key,result);
            }
        }

    public static void main(String[] args)throws IOException{
        String hdfs="hdfs://127.0.0.1:9000/";
        String input=hdfs+"log_kpi/";
        String output=hdfs+"output_pv";
        Random random=new Random();
        output+=random.nextInt();
        Configuration jobConf=new Configuration();

        jobConf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
        jobConf.set("yarn.resourcemanager.hostname", "localhost");
        jobConf.addResource("classpath:/hadoop/core-site.xml");
        jobConf.addResource("classpath:/hadoop/mapred-site.xml");
        jobConf.addResource("classpath:/hadoop/hdfs-site.xml");
        jobConf.addResource("classpath:/hadoop/yarn-site.xml");

        Job job= Job.getInstance(jobConf,KPIPV.class.getSimpleName());
        //
        job.setJarByClass(KPIPV.class);
        job.setMapperClass(KPIPVMapper.class);
        job.setReducerClass(KPIPVReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job,new Path(input));
        FileOutputFormat.setOutputPath(job,new Path(output));

        try {
            System.exit(job.waitForCompletion(true)?0:1);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
