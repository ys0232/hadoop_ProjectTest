package org.yolin.moviesRecommand;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;

public class step3 {
    public static class Step31_UserVectorSplitterMapper
    extends Mapper<LongWritable,Text,IntWritable,Text>{
        //input是step1的output（按用户分组(key=userid,value=itemid1:pref1,itemid2:pref2,...)）,
        // output是对于每一个item，用户和他给的评分(itemid,userid:pref)
        //key=itemid,value=userid:pref
        private final static IntWritable k=new IntWritable();
        private final static Text v=new Text();
        public void map(LongWritable key,Text values,Mapper<LongWritable,Text,IntWritable,Text>.Context context) throws IOException, InterruptedException {
            String[] tokens= Recommand.DELIMITER.split(values.toString());
            for (int i=1;i<tokens.length;i++){
                String[] vector=tokens[i].split(":");
               int itemID=Integer.parseInt(vector[0]);
               String pref=vector[1];
               k.set(itemID);
               v.set(tokens[0]+":"+pref);
               context.write(k,v);
            }
        }
    }
    public static class Step32_CooccurColWrapperMapper
            extends Mapper<LongWritable,Text,Text,IntWritable>{
        //input是被同一个用户评分的两个item的组合,key=item1:item2,value=concurr times
        //output is the same as step2
            private final static Text k=new Text();
            private final static IntWritable v=new IntWritable();
            public void map(LongWritable key,Text values,
                            Mapper<LongWritable,Text,Text,IntWritable>.Context context) throws IOException, InterruptedException {
                String[] tokens=Recommand.DELIMITER.split(values.toString());
                k.set(tokens[0]);
                v.set(Integer.parseInt(tokens[1]));
                context.write(k,v);
            }
    }
    public static void run1(Map<String,String> path) throws IOException, ClassNotFoundException, InterruptedException {

        Job job=Recommand.config();
        String input=path.get("step3Input1");
        String output=path.get("step3Output1");
        hdfsDAO hdfsdao=new hdfsDAO(Recommand.HDFS,job.getConfiguration());
        hdfsdao.rmr(output);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step31_UserVectorSplitterMapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path(input));
        FileOutputFormat.setOutputPath(job,new Path(output));

        boolean isCompleted=job.waitForCompletion(true);
        System.out.println(isCompleted);
    }
    public static void run2(Map<String,String> path) throws IOException, ClassNotFoundException, InterruptedException {
       Job job = Recommand.config();
       String input=path.get("step3Input2");
       String output=path.get("step3Output2");

       hdfsDAO hdfsdao=new hdfsDAO(Recommand.HDFS,job.getConfiguration());
       hdfsdao.rmr(output);

       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);

       job.setMapperClass(Step32_CooccurColWrapperMapper.class);

       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);

       FileInputFormat.setInputPaths(job,new Path(input));
       FileOutputFormat.setOutputPath(job,new Path(output));

       boolean isCompleted=job.waitForCompletion(true);
       System.out.println(isCompleted);

    }
}
