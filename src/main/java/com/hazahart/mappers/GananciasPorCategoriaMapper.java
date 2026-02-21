package com.hazahart.mappers;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.hazahart.utils.CSVUtils;

public class GananciasPorCategoriaMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private static final int SOLD = 8;
    private static final int PRICE = 9;
    private static final int CATEGORY = 14;

    @Override
    protected void map(LongWritable offset, Text lineaCsv, Context context) throws IOException, InterruptedException {
        String linea = lineaCsv.toString();

        if (offset.get() == 0 && linea.contains("id,storeId"))
            return;

        try {
            String[] columnas = CSVUtils.parse(linea);
            if (columnas.length > CATEGORY) {
                String category = columnas[CATEGORY].trim();

                String soldStr = columnas[SOLD].replace("sold", "").trim();
                double sold = soldStr.isEmpty() ? 0 : Double.parseDouble(soldStr);

                double price = Double.parseDouble(columnas[PRICE].trim());

                double revenue = price * sold;
                context.write(new Text(category), new DoubleWritable(revenue));
            }
        } catch (Exception ignored) {
        }
    }
}