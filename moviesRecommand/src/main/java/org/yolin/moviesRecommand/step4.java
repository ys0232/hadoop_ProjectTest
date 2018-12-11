package org.yolin.moviesRecommand;

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
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;

class Cooccuerrence {
    private int itemID1, itemID2, num;

    public Cooccuerrence(int itemID1, int itemID2, int num) {
        super();
        this.itemID1 = itemID1;
        this.itemID2 = itemID2;
        this.num = num;
    }

    public int getItemID1() {
        return itemID1;
    }

    public void setItemID1(int itemID1) {
        this.itemID1 = itemID1;
    }

    public int getItemID2() {
        return itemID2;
    }

    public void setItemID2(int itemID2) {
        this.itemID2 = itemID2;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}

public class step4 {

    public static class Step4_PartialMulMapper
            extends Mapper<LongWritable, Text, IntWritable, Text> {
        //input is step2's output, key=item1:item2,value=concurrtimes
        // and step31's output,key=itemid,value=userid:pref
        //output
        private final static IntWritable k = new IntWritable();
        private final static Text v = new Text();
        private final static Map<Integer, ArrayList<Cooccuerrence>> concurrMatrix = new HashMap<>();

        public void map(LongWritable key, Text values, Mapper<LongWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            String[] tokens = Recommand.DELIMITER.split(values.toString());
            //tokens[]=[itemid,userid:pref]
            //tokens[]=[item1:item2,concurrtimes]
            String[] v1 = tokens[0].split(":");
            String[] v2 = tokens[1].split(":");

            if (v1.length > 1) {
                //this is input from step2
                int itemID1 = Integer.parseInt(v1[0]);
                int itemID2 = Integer.parseInt(v1[1]);
                int num = Integer.parseInt(tokens[1]);

                ArrayList<Cooccuerrence> list = new ArrayList<>();
                if (concurrMatrix.containsKey(itemID1)) {
                    list = concurrMatrix.get(itemID1);
                }
                list.add(new Cooccuerrence(itemID1, itemID2, num));
                concurrMatrix.put(itemID1, list);
            }
            if (v2.length > 1) {
                int itemID = Integer.parseInt(tokens[0]);
                int userID = Integer.parseInt(v2[0]);
                double pref = Double.parseDouble(v2[1]);
                k.set(userID);
                if (concurrMatrix.containsKey(itemID)) {
                    for (Cooccuerrence co : concurrMatrix.get(itemID)) {
                        v.set(co.getItemID2() + "," + pref * co.getNum());
                        context.write(k, v);
                    }
                }
            }
        }
    }

    public static class Step4_AggregateAndRecommandReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        private final static Text v = new Text();

        public void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {

            Map<String, Double> result = new HashMap<>();
            for (Iterator val = values.iterator(); val.hasNext(); ) {
                String[] str = val.next().toString().split(",");
                if (result.containsKey(str[0])) {
                    result.put(str[0], result.get(str[0]) + Double.parseDouble(str[1]));
                } else {
                    result.put(str[0], Double.parseDouble(str[1]));
                }
            }
            Iterator iter = result.keySet().iterator();
            while (iter.hasNext()) {
                String itemID = (String) iter.next();
                double score = result.get(itemID);
                v.set(itemID + "," + score);
                context.write(key, v);
                System.out.println(key+"\t"+v);
            }
        }
    }

    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Recommand.config();

        String input1 = path.get("step4Input1");
        String input2 = path.get("step4Input2");
        String output = path.get("step4Output");

        hdfsDAO hdfsdao = new hdfsDAO(Recommand.HDFS, job.getConfiguration());
        hdfsdao.rmr(output);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step4_PartialMulMapper.class);
        //job.setCombinerClass(Step4_AggregateAndRecommandReducer.class);
        job.setReducerClass(Step4_AggregateAndRecommandReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path[] paths = {new Path(input1), new Path(input2)};
        FileInputFormat.setInputPaths(job, paths);
        FileOutputFormat.setOutputPath(job, new Path(output));
        //LazyOutputFormat.setOutputFormatClass(job,TextOutputFormat.class);

        boolean isCompleted = job.waitForCompletion(true);
        System.out.println(isCompleted);

    }

}
