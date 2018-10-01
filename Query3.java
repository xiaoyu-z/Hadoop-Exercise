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

public class Query3 {

    public static class CustomerMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private Text id = new Text();
        private Text name = new Text();
        private Text salary = new Text();
        //mapper the customer
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            //StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            String[] data = value.toString().split(",");
            id.set(data[0]);
            name.set(data[1]);
            salary.set(data[5]);

            context.write(id, new Text(name.toString() + "," + salary.toString()));

        }
    }

    public static class TransactionMapper
            extends Mapper<LongWritable, Text, Text, Text> {


        private Text transaction_id = new Text();
        private Text customer_id = new Text();
        private Text trans_total = new Text();
        private Text trans_items = new Text();
        //mapper the transaction
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            transaction_id.set(itr.nextToken());
            customer_id.set(itr.nextToken());
            trans_total.set(itr.nextToken());
            trans_items.set(itr.nextToken());
            //name.set(""+itr.nextToken());
            context.write(customer_id, new Text(trans_total.toString() + ":" + trans_items.toString()));

        }
    }

    public static class CustomerReducer extends Reducer<Text, Text, Text, Text> {
        private Text name = new Text();
        private Text salary = new Text();
        //perform the query
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float total = 0.0f;
            int count = 0;
            int min = 0;
            //String true_name = "?";
            for (Text val : values) {
                String value = val.toString();

                if (value.indexOf(",") >= 0) {
                    StringTokenizer itr = new StringTokenizer(value, ",");
                    name.set(itr.nextToken());
                    salary.set(itr.nextToken());
                } else {
                    StringTokenizer itr = new StringTokenizer(value, ":");
                    float trans_amount = Float.parseFloat(itr.nextToken());
                    int num_item = Integer.valueOf(itr.nextToken());
                    if (count == 0) {
                        min = num_item;
                    }
                    total += trans_amount;
                    count += 1;
                    if (num_item < min) {
                        min = num_item;
                    }
                }
            }

            context.write(key, new Text(name.toString() + "," + salary.toString() + "," + String.valueOf(count) + "," + String.valueOf(total) + "," + String.valueOf(min)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        if (args.length != 3) {
            System.err.println("Usage: <HDFS input file1> <HDFS input file1> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "query3");
        job.setJarByClass(Query3.class);
        //job.setCombinerClass(CustomerReducer.class);
        job.setReducerClass(CustomerReducer.class);
        job.setOutputKeyClass(Text.class);
        //job.setNumReduceTasks(1);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);
        //FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
