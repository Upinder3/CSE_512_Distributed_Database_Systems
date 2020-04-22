import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class equijoin {

	public static class Map extends Mapper<Text, Text, Text, Text>{
		//Mapper will categorise the data based on the join_column
		//Example: Input:
		//R, 3, Sal, Maglite, Nutley, 555-6905
		//S, 3, 24000, 5000, part1
		//Map will produce:
		//3 : R, 3, Sal, Maglite, Nutley, 555-6905
		//    S, 3, 24000, 5000, part1

		Text join_column = new Text();

		public void map(Text key, Text val, Context context) throws IOException, InterruptedException {
			if(key.toString().split(",").length >1){
				join_column.set(key.toString().split(",")[1]);
				context.write(join_column, key);
			}
			else{
				System.out.println("The input data is small.");
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> 
	{
		//For each Key two lists are created against tbl_1 and tbl_2.
		//Combining both will yield the desired result.

		Text result = new Text();
		ArrayList<String> tbl_1;
		ArrayList<String> tbl_2;

		public void reduce(Text key, Iterable<Text> vals, Context context) throws IOException, InterruptedException {
			tbl_1 = new ArrayList<String>();
			tbl_2 = new ArrayList<String>();
			for (Text line : vals){
				String tbl = line.toString().split(",")[0];
				if(tbl_1.size()==0){
					tbl_1.add(line.toString());
				}
				else{
					if(tbl_1.get(0).split(",")[0].equals(tbl)){
						tbl_1.add(line.toString());
					}
					else{
						tbl_2.add(line.toString());
					}

				}
			}
			if(tbl_1.size() > 0 && tbl_2.size() >0){
				for(int i=0;i<tbl_1.size();i++){
					for (int j=0;j<tbl_2.size();j++){
						result.set(tbl_1.get(i)+","+tbl_2.get(j));
						context.write(null, result);
						result.clear();
					}
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Equi Join");
		job.setJarByClass(equijoin.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[args.length-2]));
		FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
