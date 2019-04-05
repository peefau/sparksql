package com.inspur.tax;

import com.inspur.tax.hbase.ClientUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
@SuppressWarnings(value={"unchecked","deprecation"})
public class BulkLoadJob {
    private static Logger logger = LoggerFactory.getLogger(BulkLoadJob.class);
    
    public static class BulkLoadMap extends
            Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            
            String[] valueStrSplit = value.toString().split("\t");
            String hkey = valueStrSplit[0];
            String family = valueStrSplit[1].split(":")[0];
            String column = valueStrSplit[1].split(":")[1];
            String hvalue = valueStrSplit[2];
            final byte[] rowKey = Bytes.toBytes(hkey);
            final ImmutableBytesWritable HKey = new ImmutableBytesWritable(rowKey);
            Put HPut = new Put(rowKey);
            byte[] cell = Bytes.toBytes(hvalue);
            HPut.add(Bytes.toBytes(family), Bytes.toBytes(column), cell);
            context.write(HKey, HPut);
            
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = ClientUtils.initConfig();
        String inputPath = args[0];
        String outputPath = args[1];
        HTable hTable = null;
        try {
            System.out.println("conf = " + ClientUtils.initKerberos(conf));
            Job job = Job.getInstance(ClientUtils.initKerberos(conf), "ExampleRead");
            job.setJarByClass(BulkLoadJob.class);
            job.setMapperClass(BulkLoadJob.BulkLoadMap.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);
            // speculation
            job.setSpeculativeExecution(false);
            job.setReduceSpeculativeExecution(false);
            // in/out format
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(HFileOutputFormat2.class);
            
            FileInputFormat.setInputPaths(job, inputPath);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            
            hTable = new HTable(conf, args[2]);
            HFileOutputFormat2.configureIncrementalLoad(job, hTable);
            
            if (job.waitForCompletion(true)) {
                FsShell shell = new FsShell(conf);
                try {
                    shell.run(new String[]{"-chmod", "-R", "777", args[1]});
                } catch (Exception e) {
                    logger.error("Couldnt change the file permissions ", e);
                    throw new IOException(e);
                }
                //加载到hbase表
                LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
                loader.doBulkLoad(new Path(outputPath), hTable);
            } else {
                logger.error("loading failed.");
                System.exit(1);
            }
            
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } finally {
            if (hTable != null) {
                hTable.close();
            }
        }
    }
}