package org.yolin.moviesRecommand;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Job;
import sun.nio.ch.IOUtil;

import java.io.IOException;
import java.net.URI;

public class hdfsDAO {
    private static final String HDFS="hdfs://127.0.0.1:9000/";
    private String hdfsPath;
    private Configuration conf;
    public hdfsDAO(Configuration conf){
        this(HDFS,conf);
    }
    public hdfsDAO(String hdfs,Configuration conf){
        this.conf=conf;
        this.hdfsPath=hdfs;
    }
    public static void main(String[] args)throws IOException{
        Job job=hdfsDAO.config();
    }
    public static Job config()throws IOException{
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,hdfsDAO.class.getSimpleName());
        return job;
    }
    public  void ls(String folder)throws IOException{
        Path path=new Path(folder);
        FileSystem fs=FileSystem.get(URI.create(hdfsPath),conf);
        FileStatus[] list=fs.listStatus(path);
        System.out.println("ls : "+folder+"\n========================");
        for (FileStatus f:list){
            System.out.printf("name: %s, folder: %s, size: %d\n",f.getPath(),f.isDirectory(),f.getLen());
        }
        fs.close();
    }
    public void mkdirs(String folder)throws IOException{
        Path path=new Path(folder);
        FileSystem fs=FileSystem.get(URI.create(hdfsPath),conf);
        if (!fs.exists(path)){
            fs.mkdirs(path);
            System.out.println("Create: "+folder);
        }
        fs.close();
    }

    public void rmr(String folder)throws IOException{
        Path path=new Path(folder);
        FileSystem fs=FileSystem.get(URI.create(hdfsPath),conf);
        fs.deleteOnExit(path);
        System.out.println("Delete: "+folder);
        fs.close();
    }
    public void copyFromLocal(String local,String remote)throws IOException{
        FileSystem fs=FileSystem.get(URI.create(hdfsPath),conf);
        fs.copyFromLocalFile(new Path(local),new Path(remote));
        System.out.println("copy from: "+local+" to "+remote);
        fs.close();
    }
    public void cat(String remoteFile)throws IOException{
        Path path=new Path(remoteFile);
        FileSystem fs=FileSystem.get(URI.create(hdfsPath),conf);
        FSDataInputStream fsdis=null;
        try{
            fsdis=fs.open(path);
            IOUtils.copyBytes(fsdis,System.out,4096,false);
        }finally {
            IOUtils.closeStream(fsdis);
            fs.close();
        }
    }


}
