package com.hazahart.mappers;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.hazahart.utils.CSVUtils;

public class RatingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private static final int STORE_NAME = 2;
    private static final int RATIGN = 4;

    @Override
    protected void map(LongWritable offset, Text lineaCsv, Context context) throws IOException, InterruptedException {
        String linea = lineaCsv.toString();
        if (offset.get() == 0 && linea.contains("id,storeId"))
            return;

        try {
            String[] columnas = CSVUtils.parse(linea);
            if (columnas.length > RATIGN) {
                String storeName = columnas[STORE_NAME].trim();
                double rating = Double.parseDouble(columnas[RATIGN].trim());

                context.write(new Text(storeName), new DoubleWritable(rating));
            }
        } catch (Exception ignored) {
        }
    }
}