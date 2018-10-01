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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Query1 {

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private Text name = new Text();
        private Text id = new Text();
        private Text gender = new Text();
        private Text country_code = new Text();
        private Text salary = new Text();
        //mapper the data
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            id.set(itr.nextToken());
            name.set(itr.nextToken());
            String age = itr.nextToken();
            gender.set(itr.nextToken());
            country_code.set(itr.nextToken());
            salary.set(itr.nextToken());
            int customer_age = Integer.parseInt(age);
            if (customer_age >= 20 && customer_age <= 50) {
                //perform the query
                context.write(id, new Text(name.toString() + "," + age + "," + gender.toString() + "," + country_code.toString() + "," + salary.toString()));
            }

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        if (args.length != 2) {
            System.err.println("Usage: <HDFS input file> <HDFS output file>");
            System.exit(2);
        }
        Job job = new Job(conf, "customer_age");
        job.setJarByClass(Query1.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
