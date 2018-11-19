package com.allin.hadoop.inaction.kpi.pv.mr;
import com.allin.hadoop.inaction.kpi.dmo.RequestLog;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * PageView map/reduce程序
 */
public class PVMapperReducer {

    public static class PVMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

        private Text outKey;
        private IntWritable outVal;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            outKey = new Text();
            outVal = new IntWritable(1);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException,
                InterruptedException {
            RequestLog requestLog = RequestLog.parseLine(value.toString());

            if(null != requestLog && requestLog.getStatus() < 400){
                String url = requestLog.getUrl();
                if(url.contains("?")){

                    url = url.substring(0, url.indexOf("?"));
                }
                outKey.set(url);
                context.write(outKey, outVal);
            }
        }
    }

    public static class PVReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> iterator = values.iterator();
            int count = 0;
            while(iterator.hasNext()){
                count += iterator.next().get();
            }
            context.write(key, new IntWritable(count));

        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException,
            InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(PVMapperReducer.class);
        job.setMapperClass(PVMapper.class);
        job.setReducerClass(PVReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(PVMapperReducer.class.getClassLoader().getResource("access.log").getPath()));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        job.waitForCompletion(true);
    }
}
