/*
Bigdata pocessing Coursework
Student name: Ouphachay Thongsamouth
Student ID: 170587602
Acamedic year: 2017 - 18

*/
import java.lang.Math;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class twitterMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final IntWritable one = new IntWritable(1);
  private Text data = new Text();

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			

				String arr_tw [] = value.toString().split(";");


					if(arr_tw.length >=4 && arr_tw[2].length()<=140 && arr_tw[2].length()>0 ){

            int count_tw = (int) Math.ceil((double)arr_tw[2].length()/5);

						int max = count_tw*5;
						int start = (count_tw*5)-4;

						data.set((start)+"-"+(max));

						context.write(data, one);

					}

    }
}
