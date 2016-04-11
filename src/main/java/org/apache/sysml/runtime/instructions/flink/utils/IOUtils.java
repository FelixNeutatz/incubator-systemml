package org.apache.sysml.runtime.instructions.flink.utils;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.flink.api.java.DataSet;


public class IOUtils {
	
	public static void saveAsHadoopFile(DataSet<?> data, String path, Class outputKeyClass, Class outputValueClass, Class outputFormatClass){

		Class<?> clazz = outputFormatClass;
		Constructor<?> ctor = null;
		FileOutputFormat outFormat = null;
		try {
			ctor = clazz.getConstructor();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		}
		try {
			outFormat = (FileOutputFormat)ctor.newInstance(new FileOutputFormat[] {});
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}

		JobConf job = new JobConf();
		HadoopOutputFormat hadoopOutputFormat = new HadoopOutputFormat(outFormat, job);
		FileOutputFormat.setOutputPath(job, new Path(path));
		job.setOutputKeyClass(outputKeyClass);
		job.setOutputValueClass(outputValueClass);
		data.output(hadoopOutputFormat);
	}

	public static DataSet<?> hadoopFile(ExecutionEnvironment env, String path, Class inputFormatClass, Class inputKeyClass, Class inputValueClass){

		Class<?> clazz = inputFormatClass;
		Constructor<?> ctor = null;
		FileInputFormat inFormat = null;
		try {
			ctor = clazz.getConstructor();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		}
		try {
			inFormat = (FileInputFormat)ctor.newInstance(new FileInputFormat[] {});
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}

		JobConf job = new JobConf();
		HadoopInputFormat hadoopInputFormat = new HadoopInputFormat(inFormat, inputKeyClass, inputValueClass, job);
		FileInputFormat.addInputPath(job, new Path(path));

		DataSet<?> data = env.createInput(hadoopInputFormat);

		return data;
	}
}
