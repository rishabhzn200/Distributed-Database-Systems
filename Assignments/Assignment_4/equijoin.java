import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class equijoin {
	
	public static class JoinMapper extends Mapper<Object, Text, Text, Text>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
//			System.out.println("My Val = " + value.toString());
			
		    Text key_ = new Text();
		    Text val_ = new Text();
			
			String[] strArray = value.toString().split(",");
			
			// Trim all the tokens in the string
			for(int i=0; i<strArray.length; i++) {
				strArray[i] = strArray[i].trim().toString();
			}
			
			// strArray[0] is tablename and strArray[1] will be the unique id. This is our key
			key_.set(strArray[1]);
			val_.set(value);;
		
			
			context.write(key_, val_);
		}
	}
	
	public static class JoinReducer extends Reducer<Text,Text,Text,Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
//			System.out.println("Key = "+key);
//			for(Text value: values) {
//				System.out.println("\tVal = " + value);
//			}
			
			// Create two tables and Populate them
			List<String> R_Table = new ArrayList<String>();
			List<String> S_Table = new ArrayList<String>();
//			List<String> joinResult = new ArrayList<String>();
			boolean firstVal = true;
			String firstTablename = "";
			for(Text value: values) {
				String[] tableValues = value.toString().trim().split(",");
				
				if(firstVal) {
					firstTablename = tableValues[0];
					firstVal = false;
				}
				
				if(tableValues[0].equals(firstTablename)) {
					// Put it in R table
					R_Table.add(value.toString());
				}
				else {
					// Put it in S table
					S_Table.add(value.toString());
				}
			}
			
			// Combine the two tables and write to the output
			Text emptyText = new Text("");
			
			// If one of the table is empty, then do nothing
			if(R_Table.size() == 0 || S_Table.size() == 0) {
				;
			}
			else {
				// Cartesian product of two list
				for(String r_row: R_Table) {
					
					for(String s_row: S_Table) {
						
						String result = r_row.toString() + ", " + s_row.toString();
						
//						joinResult.add(result);
						context.write(emptyText, new Text(result));
						//System.out.println("Result ** = "+ result);
					}
				}
			}
			
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "EquiJoin");
		job.setJarByClass(equijoin.class);
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		
		// Adding Input and Output
//		FileInputFormat.addInputPath(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

