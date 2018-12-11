package org.yolin.moviesRecommand;

import jdk.internal.org.objectweb.asm.commons.Remapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.print.DocFlavor;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class step2 {
    public static class Step2_UserVectorToConcurrMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        //input是step1的output（按用户分组）key=userid,value=itemid1:pref1,itemid2:pref2...
        // output是被同一个用户评分的两个item的组合,key=item1:item2,value=concurr times
        private final static Text k=new Text();
        private final static IntWritable v=new IntWritable(1);

        public void map(LongWritable key,Text values,Mapper<LongWritable,Text,Text,IntWritable>.Context context)
                throws IOException, InterruptedException {
            String[] tokens=Recommand.DELIMITER.split(values.toString());
            for (int i=1;i<tokens.length;i++){
                String itemID=tokens[i].split(":")[0];
                for (int j=1;j<tokens.length;j++){
                    //if (i==j)continue;
                    String itemID2=tokens[j].split(":")[0];
                    k.set(itemID+":"+itemID2);
                    context.write(k,v);
                }
            }

        }
    }
    public static class Step_UserVectorToConccurReducer
            extends Reducer<Text,IntWritable,Text,IntWritable>{
        //求item组合同时出现的次数
        private IntWritable result=new IntWritable();
        public void reduce(Text key,Iterable<IntWritable> values,Reducer<Text,IntWritable,Text,IntWritable>.Context context)
                throws IOException, InterruptedException {
            int sum=0;
            for (Iterator value=values.iterator();value.hasNext();){
                IntWritable tmp=(IntWritable) value.next();
                sum+=tmp.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }
    public static void run(Map<String,String> path) throws IOException, ClassNotFoundException, InterruptedException {
        //input是step1的output（按用户分组）key=userid,value=itemid1:pref1,itemid2:pref2...
        // output是被同一个用户评分的两个item的组合,key=item1:item2,value=concurrtimes
        Job job= Recommand.config();
        String input=path.get("step2Input");
        String output=path.get("step2Output");
        hdfsDAO hdfsdao=new hdfsDAO(Recommand.HDFS,job.getConfiguration());
        hdfsdao.rmr(output);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Step2_UserVectorToConcurrMapper.class);
        job.setCombinerClass(Step_UserVectorToConccurReducer.class);
        job.setReducerClass(Step_UserVectorToConccurReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path(input));
        FileOutputFormat.setOutputPath(job,new Path(output));

        boolean isCompleted=job.waitForCompletion(true);
        System.out.println(isCompleted);
    }
}
