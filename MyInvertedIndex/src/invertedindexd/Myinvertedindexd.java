package invertedindexd;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.io.File;
import java.util.Scanner;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Myinvertedindexd extends Configured implements Tool {
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new Myinvertedindexd(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "Myinvertedindexd");
      job.setJarByClass(Myinvertedindexd.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      job.setCombinerClass(Reduce.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      //job.getConfiguration().set("mapreduce.map.output.compress", "true");
      //job.getConfiguration().set("mapreduce.output.fileoutputformat.compress", "false");
      //job.getConfiguration().set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
      job.setNumReduceTasks(10);
      //job.setNumReduceTasks(50);
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text file = new Text();

	      static List<String> stopword = new ArrayList<String>();

	      static{
	          String temp = new String();
	          try { 
		          Scanner scanner = new Scanner(new File("/home/cloudera/workspace/MyInvertedIndex/StopWords.csv"));
		          while(scanner.hasNextLine()){
		        	  temp = scanner.nextLine();
		              stopword.add(temp.substring(0, temp.indexOf(",")));
		          }
		          scanner.close();
	          } catch (FileNotFoundException e) {
	        	  System.out.println("Stopwords file not found");
	          }
	      }		
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			// Line from stackoverflow to get the filename
			file = new Text(((FileSplit) context.getInputSplit()).getPath().getName());

	    	for (String token: value.toString().split("\\s+")) {
	    		token = token.replaceAll("[^a-zA-Z]","");
	    		token = token.toLowerCase();
	            if(!stopword.contains(token))
	            {
	                word.set(token);
	            }
	      	}
	    	  
			context.write(word, file);
			
		}
	}

   public static class Reduce extends Reducer<Text, Text, Text, Text> {
      
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {
 
    	 HashMap<String, Integer> files = new HashMap<String, Integer>();
         for (Text val : values) {
        	 String strval = val.toString();
        	 int countval = 1;
        	 
        	 if (strval.contains("#")){
        		 countval = Integer.parseInt(strval.substring(strval.indexOf("#")+1, strval.length()));
        		 strval = strval.substring(0, strval.indexOf("#"));
        	 }
        	 if (!files.containsKey(strval)){
        		 files.put(strval, countval);
        	 }
        	 else {
        		 files.put(strval, files.get(strval) + countval);
        	 }
         }
         
    	 StringBuilder filesoutput = new StringBuilder();         
         for (String file : files.keySet()) {
        	 if (file.length() != 0) {
        		 Integer count = files.get(file);
        		 filesoutput.append(file + "#" + count + ", ");
        	 }
         }
         if (filesoutput.length()>1) {
         	filesoutput.setLength(filesoutput.length() - 2);
         }

         context.write(key, new Text(filesoutput.toString()));

      }
   }
}
