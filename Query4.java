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

public class Query4 {
    // we use two mapper and one reducer, so it is a job
    public static class CustomerMapper
            extends Mapper<LongWritable, Text, Text, Text> {


        private Text id = new Text();
        //private Text name = new Text();
        private Text country_code = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            //StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            String[] data = value.toString().split(",");
            id.set(data[0]);
            //name.set(data[1]);
            country_code.set(data[4] + ",0.00");
            context.write(id, country_code);

        }
    }

    public static class TransactionMapper
            extends Mapper<LongWritable, Text, Text, Text> {


        private Text transaction_id = new Text();
        private Text customer_id = new Text();
        private Text trans_total = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            transaction_id.set(itr.nextToken());
            customer_id.set(itr.nextToken());
            trans_total.set("0," + itr.nextToken());
            //name.set(""+itr.nextToken());
            context.write(customer_id, trans_total);

        }
    }
    //perform the query, store the information in the array
    public static class CustomerReducer extends Reducer<Text, Text, Text, Text> {
        private Text name = new Text();
        private Text salary = new Text();
        private Text trans = new Text();
        private int[] counts = new int[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        private float[] mins = new float[]{1000.0f, 1000.0f, 1000.0f, 1000.0f, 1000.0f, 1000.0f, 1000.0f, 1000.0f, 1000.0f, 1000.0f};
        private float[] maxs = new float[]{0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f};

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //float total = 0.0f;
            int count = 0;
            float min = 0.0f;
            float max = 0.0f;
            String country_code = "";
            for (Text val : values) {
                String value = val.toString();
                StringTokenizer itr = new StringTokenizer(value, ",");
                //name.set(itr.nextToken());
                String code = itr.nextToken();
                if (Integer.valueOf(code) > 0) {
                    country_code = code;
                } else {

                    float amount = Float.parseFloat(itr.nextToken());
                    if (count <= 0) {
                        //System.out.println("DOOOOOOOOOOOO");
                        max = amount;
                        min = amount;
                        count += 1;
                    } else {
                        if (max < amount) {
                            max = amount;
                        }
                        if (min > amount) {
                            min = amount;
                        }
                        //count += 1;
                    }

                }
            }
            int group_id = Integer.valueOf(country_code);
            //if(group_id>10 || group_id<1){System.out.println(group_id);}
            counts[group_id - 1] += 1;
            if (maxs[group_id - 1] < max) {
                System.out.println(max);
                maxs[group_id - 1] = max;
            }
            if (mins[group_id - 1] > min) {

                mins[group_id - 1] = min;
            }
            //context.write(key, new Text(String.valueOf(count)+","+String.valueOf(min)+","+String.valueOf(max)));
        }
        //override the method to output the query result
        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (int i = 0; i < 10; i++) {
                context.write(new Text(String.valueOf(i + 1)), new Text(String.valueOf(counts[i]) + "," + String.valueOf(mins[i]) + "," + String.valueOf(maxs[i])));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        if (args.length != 3) {
            System.err.println("Usage: customer <HDFS input file1> <HDFS input file1> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "query4");
        job.setJarByClass(Query4.class);
        //job.setCombinerClass(CustomerCombiner.class);
        job.setReducerClass(CustomerReducer.class);
        //job.setMapperClass(CustomerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        //job.setReducerClass(IntSumReducer.class);
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
