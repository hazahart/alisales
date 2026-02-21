package com.hazahart.reducers;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RatingReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text tienda, Iterable<DoubleWritable> valoraciones, Context context) throws IOException, InterruptedException {
        int prodTotal = 0;
        int mejorValorados = 0;
        double sumaRating = 0.0;

        for (DoubleWritable valoracion : valoraciones) {
            double rating = valoracion.get();
            prodTotal++;
            sumaRating += rating;
            
            if (rating >= 4.5) { 
                mejorValorados++;
            }
        }

        double averageRating = prodTotal > 0 ? sumaRating / prodTotal : 0.0;
        double percentage = prodTotal > 0 ? ((double) mejorValorados / prodTotal) * 100.0 : 0.0;

        String valueStr = String.format("Highly Rated Products: %d (%.1f%%) | Average Rating: %.2f | Total Products: %d", mejorValorados, percentage, averageRating, prodTotal);
        
        String formattedOutput = String.format("\"%s\", \"%s\"", tienda.toString(), valueStr);
        context.write(new Text(formattedOutput), NullWritable.get());
    }
}