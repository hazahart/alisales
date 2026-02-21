package com.hazahart.mappers;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.hazahart.utils.CSVUtils;

public class MasVendidoPorCategoriaMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private static final int TITLE = 3;
    private static final int SOLD = 8;
    private static final int CATEGORY = 14;

    @Override
    protected void map(LongWritable offset, Text lineaCsv, Context context) throws IOException, InterruptedException {
        String linea = lineaCsv.toString();
        
        if (offset.get() == 0 && linea.contains("id,storeId")) return;

        try {
            String[] columnas = CSVUtils.parse(linea);
            
            if (columnas.length > CATEGORY) {
                String category = columnas[CATEGORY].trim();
                String title = columnas[TITLE].trim();
                
                String soldStr = columnas[SOLD].replace("sold", "").trim();
                int sold = soldStr.isEmpty() ? 0 : Integer.parseInt(soldStr);
                
                context.write(new Text(category), new Text(title + "\t" + sold));
            }
        } catch (Exception ignored) { 
        }
    }
}