package com.hazahart;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.hazahart.mappers.GananciasPorCategoriaMapper;
import com.hazahart.reducers.GananciasPorCategoriaReducer;

public class GananciasMain {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Revenue");
        
        job.setJarByClass(GananciasMain.class);
        job.setMapperClass(GananciasPorCategoriaMapper.class);
        job.setReducerClass(GananciasPorCategoriaReducer.class);
        
        // Output Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        
        // Output Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}