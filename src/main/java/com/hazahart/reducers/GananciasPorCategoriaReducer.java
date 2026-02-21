package com.hazahart.reducers;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GananciasPorCategoriaReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text categoria, Iterable<DoubleWritable> ganancias, Context context) throws IOException, InterruptedException {
        double sum = 0;
        
        for (DoubleWritable ganancia : ganancias) {
            sum += ganancia.get();
        }
        
        String salida = String.format("\"%s\", \"$%.2f\"", categoria.toString(), sum);
        context.write(new Text(salida), NullWritable.get());
    }
}