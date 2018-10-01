package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Query5 {


    public static class TransactionMapper
            extends Mapper<LongWritable, Text, Text, Text> {


        private Text transaction_id = new Text();
        //mapper the joined data
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");
            transaction_id.set(data[0]);
            //String customer_id = data[1];
            String trans_total = data[2];
            String age = data[5];
            String gender = data[6];
            //customer_id.set(itr.nextToken());
            //trans_total.set(itr.nextToken());
            //name.set(""+itr.nextToken());
            context.write(transaction_id, new Text(age + "," + gender + "," + trans_total));

        }
    }

    public static class AgePartitioner extends Partitioner<Text, Text> {
        public int getPartition(Text key, Text value, int numReduceTasks) {
        //divide into 6 groups
            String age_string = value.toString().split(",")[0];
            int age = Integer.valueOf(age_string);
            if (10 <= age && age < 20) return 0;
            if (20 <= age && age < 30) return 1 % numReduceTasks;
            if (30 <= age && age < 40) return 2 % numReduceTasks;
            if (40 <= age && age < 50) return 3 % numReduceTasks;
            if (50 <= age && age < 60) return 4 % numReduceTasks;
            if (60 <= age && age <= 70) return 5 % numReduceTasks;
            return 0;
        }

    }

    public static class GenderReducer extends Reducer<Text, Text, Text, Text> {
        //private Text age = new Text();
        //private Text gender = new Text();

        private float[] male_totals = new float[]{0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f};
        private int[] male_counts = new int[]{0, 0, 0, 0, 0, 0};
        private float[] male_mins = new float[]{1000.0f, 1000.0f, 1000.0f, 1000.0f, 1000.0f, 1000.0f};
        private float[] male_maxs = new float[]{0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f};
        private float[] female_totals = new float[]{0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f};
        private int[] female_counts = new int[]{0, 0, 0, 0, 0, 0};
        private float[] female_mins = new float[]{1000.0f, 1000.0f, 1000.0f, 1000.0f, 1000.0f, 1000.0f};
        private float[] female_maxs = new float[]{0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f};

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //float total = 0.0f;
            //int count = 0;
            int group = 0;
            boolean male = true;
            for (Text val : values) {

                StringTokenizer itr = new StringTokenizer(val.toString(), ",");
                int age = Integer.valueOf(itr.nextToken());
                if (10 <= age && age < 20) group = 1;
                if (20 <= age && age < 30) group = 2;
                if (30 <= age && age < 40) group = 3;
                if (40 <= age && age < 50) group = 4;
                if (50 <= age && age < 60) group = 5;
                if (60 <= age && age <= 70) group = 6;
                String gender = itr.nextToken();
                float trans_total = Float.parseFloat(itr.nextToken());
                //total+=trans_total;
                //count+=1;
                //based on gender, divide another two group
                if (gender.equals("female")) male = false;
                if (male) {
                    male_counts[group - 1] += 1;
                    male_totals[group - 1] += trans_total;
                    if (trans_total > male_maxs[group - 1]) male_maxs[group - 1] = trans_total;
                    if (trans_total < male_mins[group - 1]) male_mins[group - 1] = trans_total;
                } else {
                    female_counts[group - 1] += 1;
                    female_totals[group - 1] += trans_total;
                    if (trans_total > female_maxs[group - 1]) female_maxs[group - 1] = trans_total;
                    if (trans_total < female_mins[group - 1]) female_mins[group - 1] = trans_total;

                }

            }
        }
        //float average = total/count;

        protected void cleanup(Context context) throws IOException, InterruptedException {

            int partition_num = Integer.valueOf(context.getConfiguration().get("mapred.task.partition"));
            switch (partition_num) {
                case 0:
                    context.write(new Text("[10,20)"), new Text("male," + String.valueOf(male_mins[partition_num]) + "," + String.valueOf(male_maxs[partition_num]) + "," + String.valueOf(male_totals[partition_num] / male_counts[partition_num])));
                    context.write(new Text("[10,20)"), new Text("female," + String.valueOf(female_mins[partition_num]) + "," + String.valueOf(female_maxs[partition_num]) + "," + String.valueOf(female_totals[partition_num] / female_counts[partition_num])));
                    break;
                case 1:
                    context.write(new Text("[20,30)"), new Text("male," + String.valueOf(male_mins[partition_num]) + "," + String.valueOf(male_maxs[partition_num]) + "," + String.valueOf(male_totals[partition_num] / male_counts[partition_num])));
                    context.write(new Text("[20,30)"), new Text("female," + String.valueOf(female_mins[partition_num]) + "," + String.valueOf(female_maxs[partition_num]) + "," + String.valueOf(female_totals[partition_num] / female_counts[partition_num])));
                    break;
                case 2:
                    context.write(new Text("[30,40)"), new Text("male," + String.valueOf(male_mins[partition_num]) + "," + String.valueOf(male_maxs[partition_num]) + "," + String.valueOf(male_totals[partition_num] / male_counts[partition_num])));
                    context.write(new Text("[30,40)"), new Text("female," + String.valueOf(female_mins[partition_num]) + "," + String.valueOf(female_maxs[partition_num]) + "," + String.valueOf(female_totals[partition_num] / female_counts[partition_num])));
                    break;
                case 3:
                    context.write(new Text("[40,50)"), new Text("male," + String.valueOf(male_mins[partition_num]) + "," + String.valueOf(male_maxs[partition_num]) + "," + String.valueOf(male_totals[partition_num] / male_counts[partition_num])));
                    context.write(new Text("[40,50)"), new Text("female," + String.valueOf(female_mins[partition_num]) + "," + String.valueOf(female_maxs[partition_num]) + "," + String.valueOf(female_totals[partition_num] / female_counts[partition_num])));
                    break;
                case 4:
                    context.write(new Text("[50,60)"), new Text("male," + String.valueOf(male_mins[partition_num]) + "," + String.valueOf(male_maxs[partition_num]) + "," + String.valueOf(male_totals[partition_num] / male_counts[partition_num])));
                    context.write(new Text("[50,60)"), new Text("female," + String.valueOf(female_mins[partition_num]) + "," + String.valueOf(female_maxs[partition_num]) + "," + String.valueOf(female_totals[partition_num] / female_counts[partition_num])));
                    break;
                case 5:
                    context.write(new Text("[60,70]"), new Text("male," + String.valueOf(male_mins[partition_num]) + "," + String.valueOf(male_maxs[partition_num]) + "," + String.valueOf(male_totals[partition_num] / male_counts[partition_num])));
                    context.write(new Text("[60,70]"), new Text("female," + String.valueOf(female_mins[partition_num]) + "," + String.valueOf(female_maxs[partition_num]) + "," + String.valueOf(female_totals[partition_num] / female_counts[partition_num])));
                    break;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        if (args.length != 2) {
            System.err.println("Usage:  <HDFS input file1> <HDFS input file1> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "query5");
        job.setJarByClass(Query5.class);
        //job.setCombinerClass(CustomerReducer.class);
        //job.setReducerClass(CustomerReducer.class);
        job.setMapperClass(TransactionMapper.class);
        job.setPartitionerClass(AgePartitioner.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(GenderReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(6);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
