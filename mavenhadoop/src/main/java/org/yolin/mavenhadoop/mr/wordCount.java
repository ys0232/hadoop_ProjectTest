//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//
package org.yolin.mavenhadoop.mr;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class wordCount {
    public wordCount() {
    }
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final static IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;

            IntWritable val;
            for(Iterator var5 = values.iterator(); var5.hasNext(); sum += val.get()) {
                val = (IntWritable)var5.next();
            }

            result.set(sum);
            context.write(key, result);
        }
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final static Text word = new Text();

        public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while(itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }

        }
    }

    public static void main(String[] args) throws Exception {

        String input="hdfs://127.0.0.1:9000/input";
        String output="hdfs://127.0.0.1:9000/output/res";
        Random rand=new Random();
        output=output+rand.nextInt();
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://127.0.0.1:9000");
//        conf.set("yarn.resourcemanager.hostname", "localhost");
//        conf.addResource("classpath:/hadoop/core-site.xml");
//        conf.addResource("classpath:/hadoop/mapred-site.xml");
//        conf.addResource("classpath:/hadoop/hdfs-site.xml");
//        conf.addResource("classpath:/hadoop/yarn-site.xml");

        Job job = Job.getInstance(conf,"wordCountJob");
        job.setJarByClass(wordCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        //log4j打印输出
       // BasicConfigurator.configure();
        //System.out.println(job.waitForCompletion(true));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
