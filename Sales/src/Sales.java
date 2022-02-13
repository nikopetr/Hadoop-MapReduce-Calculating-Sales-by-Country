import java.io.IOException;
// import java.util.*;

import java.io.DataInput;
import java.io.DataOutput;
        
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

// EXPLANATION OF WRITABLE CLASSES/OBJECTS: Why we use IntWritable instead of int or Integer? Why we use LongWritable instead of Long? (https://community.cloudera.com/t5/Support-Questions/Why-we-use-IntWritable-instead-of-Int-Why-we-use/td-p/228098#:~:text=%22int%22%20is%20a%20primitive%20type,serialization%20in%20the%20Hadoop%20environment.)

// Task: Sales - Num of products / country
// Find the number of products and the sum of sales per country, given the input file SalesJan2009.csv.
//
// Output file example (part-r-00000):
// Argentina 1 1200
// Australia 38 64800
// Austria 7 10800
// …
//
// The idea of this problem's solution is to use the same Key for every different country.
// In addition, the value used at each mapper will be the price (sales) of that row, which corresponds to the Key country.
public class Sales {

// The objects of this class will be utilized, since for this task we would like to output multiple values for each key.
// A custom hadoop writable data type which needs to be used as value field 
// in Mapreduce programs must implement Writable interface org.apache.hadoop.io.Writable.
 public static class CountrySalesStatsWritable implements Writable {
 	   private IntWritable productCount;
	   private LongWritable priceSum;
	 
	   // Default Constructor
	   public CountrySalesStatsWritable() 
	   {
			this.productCount = new IntWritable();
			this.priceSum = new LongWritable();
	   }
	 
	   // Custom Constructor
	   public CountrySalesStatsWritable(IntWritable productCount, LongWritable priceSum) 
	   {
			this.productCount = productCount;
			this.priceSum = priceSum;
	   }
	 
	   @Override
	   // Overriding default readFields method.
	   // Method that de-serializes the byte stream data
	   public void readFields(DataInput in) throws IOException 
	   {
			productCount.readFields(in);
			priceSum.readFields(in);
	   }
	 
	   @Override
	   // Overriding default write method.
	   // Method that serializes object data into byte stream data
	   public void write(DataOutput out) throws IOException 
	   {
			productCount.write(out);
			priceSum.write(out);
	   }	   
	   
	   @Override
	   // Overriding default toString method.
	   public String toString() {
		   return productCount.toString() + " " + priceSum.toString();
		}
 }


// KVs input types to Mapper are: <K:LongWritable, V:Text> (by default)
// KVs output types from Mapper are: <K:Text, V:LongWritable>	
 public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> { 
    private Text country = new Text();
    private LongWritable price; // Price
    
    // Overwritten method of Mapper
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	// Note: For this case of map-phase, the input key is "ignored" since it is just the line-offset, and it is not being used   
        String line = value.toString(); // Transform the value (input line) from Text object to String
        String[] columns = line.split(",");
        
        country.set(columns[7]); // Country name
        price = new LongWritable(Long.parseLong(columns[2])); // Price
        // System.out.println("Country read: " + country.toString() + ", Price read: " + price.toString());
        context.write(country, price); // This context object represents the Key-Value pair output of the mapper (Context object: Allows the Mapper/Reducer to interact with the rest of the Hadoop system)
        
    }
 } 

// KVs input types to Reducer are: <K:Text, V:list(LongWritable)>
// KVs output types from Reducer are: <K:Text, V:CountrySalesStatsWritable>
 public static class Reduce extends Reducer<Text, LongWritable, Text, CountrySalesStatsWritable> {
    
    // Overwritten method of Reducer
    public void reduce(Text key, Iterable<LongWritable> values, Context context) // values should contain the read prices with the same key (country)
      throws IOException, InterruptedException {
        int productCount = 0;
        long priceSum = 0;
        
        for (LongWritable price : values) {
        	productCount++;
        	priceSum += price.get();
        }
        
        // Since there are multiple output values required, an object of a custom-made class CountrySalesStatsWritable is created 
        CountrySalesStatsWritable countrySalesStatsWritable = new CountrySalesStatsWritable(new IntWritable(productCount), new LongWritable (priceSum));
        context.write(key, countrySalesStatsWritable); // This context object represents the Key-Value pair output of the reducer and it is going to be given as the final output (Context object: Allows the Mapper/Reducer to interact with the rest of the Hadoop system)
    }
    
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
   
    Job job = Job.getInstance(conf, "sales"); // The job is used like a wrapper, the name of the job is just for seeing which program-job is running
    job.setJarByClass(Sales.class);
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class); // Mapper output value class, also input value class of reducer
        
    job.setMapperClass(Map.class);
    //job.setCombinerClass(Reduce.class); // Don’t reuse the Reducer for Combiner since the Reducer’s input and output key value pair types do not match
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path("hdfs://localhost:54310/user/csdeptucy/input/SalesJan2009.csv"));
    FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:54310/user/csdeptucy/output_sales"));
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
 }        
}