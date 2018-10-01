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

public class Query2 {

    public static class CustomerMapper
            extends Mapper<LongWritable, Text, Text, Text> {


        private Text id = new Text();
        private Text name = new Text();
        //mapper the cusmtomers
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            id.set(itr.nextToken());
            name.set(itr.nextToken() + ",0,0.00");// this data will not influence the transaction data
            context.write(id, name);
        }
    }

    public static class TransactionMapper
            extends Mapper<LongWritable, Text, Text, Text> {


        private Text transaction_id = new Text();
        private Text customer_id = new Text();
        private Text trans_total = new Text();
        //mapper the transactions
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            transaction_id.set(itr.nextToken());
            customer_id.set(itr.nextToken());
            trans_total.set("?,1," + itr.nextToken());//set as name, count, total
            //name.set(""+itr.nextToken());
            context.write(customer_id, trans_total);
        }
    }

    public static class CustomerReducer extends Reducer<Text, Text, Text, Text> {
        //private Text name = new Text();
        //perform the query
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            float total = 0.0f;
            int count = 0;
            String true_name = "?";
            for (Text val : values) {
                String value = val.toString();
                //if(value.indexOf(":")==-1){
                StringTokenizer itr = new StringTokenizer(value, ",");
                String cust_name = itr.nextToken();
                if (cust_name.length() >= 10) {
                    true_name = cust_name;
                }
                count += Integer.valueOf(itr.nextToken());
                total += Float.parseFloat(itr.nextToken());

            }
            context.write(key, new Text(true_name + "," + String.valueOf(count) + "," + String.valueOf(total)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        if (args.length != 3) {
            System.err.println("Usage: <HDFS input file1> <HDFS input file1> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "customer_trans");
        job.setJarByClass(Query2.class);
        job.setCombinerClass(CustomerReducer.class);
        //use the combiner
        job.setReducerClass(CustomerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
