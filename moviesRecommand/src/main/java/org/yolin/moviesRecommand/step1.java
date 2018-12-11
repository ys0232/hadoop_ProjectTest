package org.yolin.moviesRecommand;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class step1 {
    public static class step_toItemPreMapper extends Mapper<Object,Text,IntWritable,Text>{

        private final static IntWritable k=new IntWritable();
        private final static Text v=new Text();
        @Override
        public void map(Object object,Text value,Mapper<Object,Text,IntWritable,Text>.Context context)
        throws IOException,InterruptedException{
            String[] tokens=Recommand.DELIMITER.split(value.toString());
            int userID=Integer.valueOf(tokens[0]);
            String itemID=tokens[1];
            String pref=tokens[2];
            k.set(userID);
            v.set(itemID+":"+pref);
            context.write(k,v);
        }
    }
    public static class step_ToUserVectorReducer extends Reducer<IntWritable,Text,IntWritable,Text> {

        private final static Text v=new Text();
        public void reduce(IntWritable key, Iterable<Text> value,Reducer<IntWritable,Text,IntWritable,Text>.Context context)
        throws IOException,InterruptedException{
            //相当于groupby userid，将同一个用户评分的item和对应的评分写到一起
            StringBuilder sb=new StringBuilder();
            for (Iterator val=value.iterator();val.hasNext();) {
                sb.append(","+val.next());
            }
            v.set(sb.toString().replaceFirst(",",""));
            context.write(key,v);
        }
    }
    public static void run(Map<String,String> path) throws IOException, ClassNotFoundException, InterruptedException {
        //按用户记录itemid和评分,input value=userid itemid pref
        //output key=userid,value=itemid1:pref1,itemid2:pref2...
        Job job=Recommand.config();
        String input=path.get("step1Input");
        String output=path.get("step1Output");

        hdfsDAO hdfsdao=new hdfsDAO(Recommand.HDFS,job.getConfiguration());
        hdfsdao.rmr(input);
        hdfsdao.mkdirs(input);
        hdfsdao.copyFromLocal(path.get("data"),input);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(step_toItemPreMapper.class);
        job.setReducerClass(step_ToUserVectorReducer.class);
        job.setCombinerClass(step_ToUserVectorReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path(input));
        FileOutputFormat.setOutputPath(job,new Path(output));

        boolean isCompleted=job.waitForCompletion(true);
        if (isCompleted){
            System.out.println("step1 is running ");
        }
    }
}
