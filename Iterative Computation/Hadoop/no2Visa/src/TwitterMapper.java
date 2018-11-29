// deni setiawan msc software engineering

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Calendar;
import java.util.Hashtable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
//
import java.util.*;

import org.apache.hadoop.io.NullWritable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;


public class TwitterMapper extends Mapper<Object, Text, Text, IntWritable> {


  private Hashtable<String, String> companyInfo0;
  private Hashtable<String, String> companyInfo1;
  private Hashtable<String, String> companyInfo2;
  private Hashtable<String, String> companyInfo3;
  private Hashtable<String, String> companyInfo4;

    private final IntWritable one = new IntWritable(1);
    private IntWritable two = new IntWritable(1);
    private final IntWritable three = new IntWritable(1);
    private final IntWritable four = new IntWritable(1);
    private final IntWritable five = new IntWritable(1);
    private final IntWritable six = new IntWritable(1);
    private final IntWritable seven = new IntWritable(1);
    private final IntWritable eight = new IntWritable(1);
    private Text data = new Text();
    private Text val = new Text();
    int saya;
    String keyzz;


        @Override
      	protected void setup(Context context) throws IOException, InterruptedException {

          companyInfo0 = new Hashtable<String, String>();
          companyInfo1 = new Hashtable<String, String>();
      		companyInfo2 = new Hashtable<String, String>();

      		// We know there is only one cache file, so we only retrieve that URI
      		URI fileUri = context.getCacheFiles()[0];

      		FileSystem fs = FileSystem.get(context.getConfiguration());
      		FSDataInputStream in = fs.open(new Path(fileUri));

      		BufferedReader br = new BufferedReader(new InputStreamReader(in));

      		String line = null;
      		try {
      			// we discard the header row
      			br.readLine();
            int a = 1;
            int b = 1;
            int c = 1;
            int d = 1;
            int e = 1;
            int f = 1;
            int g = 1;
            int h = 1;

      			while ((line = br.readLine()) != null) {
    //lalalal
            	context.getCounter(CustomCounters.NUM_COMPANIES).increment(1);

      					//id,name,nationality,sex,dob,height,weight,sport,gold,silver,bronze
      					// 736041664,A Jesus Garcia,ESP,male,10/17/69,1.72,64,athletics,0,0,0

      				String[] fields = line.split(",");

            	// Fields are: 0:Symbol 1:Name 2:IPOyear 3:Sector 4:industry

      				if (fields.length >= 5 ){
                 // main logic. masukkan semua disini

                  if(companyInfo0.containsKey(fields[2]) == false){

                    companyInfo0.put(fields[2], Integer.toString(a));
                    
                    a++;

                  };

                  //
                  // if(companyInfo1.containsKey(fields[3]) == false){
                  //
                  //   companyInfo1.put(fields[3], Integer.toString(b));
                  //   b++;
                  //
                  // };
                  //
                  //
                  // if(companyInfo2.containsKey(fields[4]) == false){
                  //
                  //   companyInfo2.put(fields[4], Integer.toString(b));
                  //   b++;
                  //
                  // };
                  //
                  // if(companyInfo.containsKey(fields[4]) == false){
                  //
                  //   companyInfo.put(fields[4], Integer.toString(a));
                  //   a++;
                  //
                  // };


              }
      														//keys    , // value

      			}
      			br.close();
      		} catch (IOException e1) {
      		}

      		super.setup(context);
      	} // end of hash



    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      try {


                String[] itr = value.toString().split(","); // exploed and parsed to array and data type is string fix

                if(itr.length >= 4){

                  // keyzz = companyInfo.get(itr[0]);
                  //
                  // //data.set(keyzz); // Set name of athlete to text
                  // data.set(keyzz); // Set name of athlete to text
                  //
                  // // context.write(key, 1); // 1 is count
                  // context.write(data, one ); // (key , value)
                  //
                  //
                  //


                  Set<String> keys = companyInfo0.keySet(); // get key set into array

                    for(String keyzz: keys){ // foreach array one by one

                         if(itr[2].contains(keyzz)){ // main logic --> if in the tweet consists name in key of hashtable, exeecute this

                            data.set(companyInfo0.get(itr[2])); // Set name of athlete to text
                            //companyInfo.get(itr[2])
                            // context.write(key, 1); // 1 is count
                            context.write(data, one ); // (key , value)

                        }

                    }

                    // Set<String> keys1 = companyInfo1.keySet(); // get key set into array
                    //
                    //   for(String keyzz1: keys1){ // foreach array one by one
                    //
                    //        if(itr[2].contains(keyzz1)){ // main logic --> if in the tweet consists name in key of hashtable, exeecute this
                    //
                    //           data.set(companyInfo1.get(itr[2])); // Set name of athlete to text
                    //           //companyInfo.get(itr[2])
                    //           // context.write(key, 1); // 1 is count
                    //           context.write(data, one ); // (key , value)
                    //
                    //       }
                    //
                    //   }

                    //  if(itr[3].contains(keyzz)){ // main logic --> if in the tweet consists name in key of hashtable, exeecute this
                    //
                    //     data.set(companyInfo.get(itr[3])); // Set name of athlete to text
                    //     //companyInfo.get(itr[2])
                    //     // context.write(key, 1); // 1 is count
                    //     context.write(data, one ); // (key , value)
                    //
                    // }

                }

        //end try
      } catch (NumberFormatException e) {
          System.err.println("NumberFormatException: " + e.getMessage());
      }//end catch

    } // end of map


}// end of class
