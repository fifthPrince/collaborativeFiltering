import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class ItemBasedCFCoCurrentMatrix {

	public static class Map extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {
		
		private Text internalKey = new Text();
		private Text internalValue = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
						throws IOException {
			
			String[] splitted = value.toString().split("\\s+");
			if(splitted.length == 3){
				internalKey.set(splitted[0]);
				internalValue.set(splitted[1]+":" +splitted[2]);
				output.collect(internalKey,internalValue);
			}			
		}
	}

	public static class Reduce extends MapReduceBase implements
	Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
						throws IOException {
			
			Text outputValue = new Text();
			Text outputEmptyValue = new Text();
			outputEmptyValue.set("");
			StringBuilder sb = new StringBuilder();
			while(values.hasNext()){	
				sb.append(values.next()).append(",");			
			}
			outputValue.set(sb.toString().substring(0,sb.length()-1));
			output.collect(outputValue,outputEmptyValue);
			//Object[] valuesArray = IteratorUtils.toArray(values);
			
//			Object[] valuesArray = iterator2Array(values);
//			for(int i = 0; i < valuesArray.length;i++){
//				String movieRatingPair = valuesArray[i].toString();
//				String[] mrArray = movieRatingPair.split(":");
//				for(int j = i+1; j < valuesArray.length; j++){
//					String movieRatingPair2 = valuesArray[j].toString();
//					String[] mrArray2 = movieRatingPair2.split(":");
//					outputValue.set("(" + mrArray[0] + "," + mrArray2[0] + ")," + "(" + mrArray[1] + "," + mrArray2[1] + ")");
//					output.collect(key, outputValue);
//				}
//			}
			
			
			
		}
		

		private Object[] iterator2Array(Iterator iter){
			
			int length = 0;
			ArrayList lst = new ArrayList();
			while(iter.hasNext()){
				Object o = iter.next();
				lst.add(o);
			}			
			return lst.toArray();
		}
	}
	

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(ItemBasedCFCoCurrentMatrix.class);
		conf.setJobName("ItemBasedCFCoCurrentMatrix");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		//conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path("hdfs://192.168.0.2:9000/sparkDir/smallest2"));
		FileOutputFormat.setOutputPath(conf, new Path("hdfs://192.168.0.2:9000/sparkDir/cocurrentmatrix/1.out"));


		JobClient.runJob(conf);
	}

}
