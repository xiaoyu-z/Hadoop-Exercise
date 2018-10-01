/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Join {

    public static class TransactionMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        //private final static IntWritable one = new IntWritable(1);
        //private Text word = new Text();
        //private Text name = new Text();
        private HashMap<String, String> customer_info = new HashMap<String, String>();
        private Text transaction_id = new Text();

        //private Text customer_id = new Text();
        //private Text trans_total = new Text();
        //private Text num_items = new Text();
        //private Text desc = new Text();
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            transaction_id.set(itr.nextToken());
            String customer_id = itr.nextToken();
            String trans_total = itr.nextToken();
            String num_items = itr.nextToken();
            String desc = itr.nextToken();
            String customer_data = customer_info.get(customer_id);
            context.write(transaction_id, new Text(customer_id + "," + trans_total + "," + num_items + "," + customer_data + "," + desc));
            //String age = itr.nextToken();
            //int customer_age = Integer.parseInt(age);
            //if(customer_age>20){
            //	context.write(id, name);
            //}
            //while (itr.hasMoreTokens()) {
            //  word.set(itr.nextToken());
            //  context.write(word, one);
            //}
        }

        public void setup(Context context) throws IOException, InterruptedException {
            String customer_file = context.getConfiguration().get("customer_file_path");

            BufferedReader b_reader = new BufferedReader(new FileReader(customer_file));
            String line = b_reader.readLine();
            while (line != null) {
                StringTokenizer itr = new StringTokenizer(line, ",");
                String id = itr.nextToken();
                String name = itr.nextToken();
                String age = itr.nextToken();
                String gender = itr.nextToken();
                String country_code = itr.nextToken();
                String salary = itr.nextToken();
                customer_info.put(id, name + "," + age + "," + gender + "," + country_code + "," + salary);
                line = b_reader.readLine();
            }

            b_reader.close();

        }

    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        if (args.length != 3) {
            System.err.println("Usage: customer <HDFS input file1> <HDFS input file1> <HDFS output file>");
            System.exit(2);
        }
        conf.set("customer_file_path", args[0]);
        Job job = new Job(conf, "join");
        job.setJarByClass(Join.class);
        //job.setCombinerClass(CustomerReducer.class);
        //job.setReducerClass(CustomerReducer.class);
        job.setMapperClass(TransactionMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        //job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        //job.setNumReduceTasks(1);
        job.setOutputValueClass(Text.class);
        //MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        //MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
