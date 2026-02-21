package com.hazahart;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.hazahart.mappers.MasVendidoPorCategoriaMapper;
import com.hazahart.reducers.MasVendidoPorCategoriaReducer;

public class MasVendidoPorCategoriaMain {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Best Product By Category");
        
        job.setJarByClass(MasVendidoPorCategoriaMain.class);
        job.setMapperClass(MasVendidoPorCategoriaMapper.class);
        job.setReducerClass(MasVendidoPorCategoriaReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}