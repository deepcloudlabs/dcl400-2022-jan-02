package com.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// SQL: select continent, sum(population) from countries group by continent

public class GetContinentPopulation {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "get continents # of countries");
		job.setJarByClass(GetContinents.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileSystem fs = outputPath.getFileSystem(new Configuration());
		if (fs.exists(outputPath))
			fs.delete(outputPath, true);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setMapperClass(ContinentPopulationMapper.class);
		job.setCombinerClass(ContinentPopulationReducer.class);
		job.setReducerClass(ContinentPopulationReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

class ContinentPopulationMapper extends Mapper<Object, Text, Text, LongWritable> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\",\"");
		long population = 0;
		try {
			population = Long.parseLong(tokens[6]);
		} catch (Exception e) {

		}
		context.write(new Text(tokens[2]), new LongWritable(population));
	}
}

class ContinentPopulationReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		long totalPopulation = 0;
		for (LongWritable value : values) {
			totalPopulation += value.get();
		}
		context.write(key, new LongWritable(totalPopulation));
	}
}