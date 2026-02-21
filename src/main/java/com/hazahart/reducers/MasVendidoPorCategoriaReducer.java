package com.hazahart.reducers;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MasVendidoPorCategoriaReducer extends Reducer<Text, Text, Text, NullWritable> {

    @Override
    protected void reduce(Text categoria, Iterable<Text> productos, Context context) throws IOException, InterruptedException {
        int cantMaxVendidas = -1;
        String masVendido = "";

        for (Text producto : productos) {
            String[] partes = producto.toString().split("\t");
            
            if (partes.length == 2) {
                String titulo = partes[0];
                int cantidadVendida = Integer.parseInt(partes[1]);
                
                if (cantidadVendida > cantMaxVendidas) {
                    cantMaxVendidas = cantidadVendida;
                    masVendido = titulo;
                }
            }
        }

        String strMejorProducto = String.format("Best Product: %s, with total sold: %d", masVendido, cantMaxVendidas);
        String salida = String.format("\"%s\", \"%s\"", categoria.toString(), strMejorProducto);
        
        context.write(new Text(salida), NullWritable.get());
    }
}