/*
Bigdata pocessing Coursework
Student name: Ouphachay Thongsamouth
Student ID: 170587602
Acamedic year: 2017 - 18

*/
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class twitterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {

    	int sum = 0;
        for (IntWritable value : values) {
            sum = sum + value.get();
        }
               result.set(sum);

        context.write(key, result);
    }
}