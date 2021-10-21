package org.example;

// Taken from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Source_Code

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountSet {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class SwapMapper extends Mapper<Object, Text, IntWritable, Text>{
        // input is text then writable and then the key and value is swapped so we can reduce
        private Text word = new Text();
        private IntWritable integer = new IntWritable(0);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] inputs = value.toString().split("\\s+");
//            System.out.println(inputs);

            try{
                integer.set(Integer.parseInt(inputs[1])); // integer comes after word
                word.set(inputs[0]);
            } catch (Exception exception){
                System.out.println(exception.getStackTrace());
            }
             // word comes before integer
            context.write(integer,word); // le flip
        }
    }

    public static class WordCombiner extends Reducer<IntWritable,Text,Text,Text> {
        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            // concatonate or list
            // but I do not think that the lists in java are ()
            // so concatonation is the best bet?

            String resultingString = "("; // initialize the string

            for(Text value : values) {
                resultingString += value + ", ";
            }
            resultingString =  resultingString.substring(0, resultingString.length() - 2); // from stack exchange
            // https://stackoverflow.com/questions/7438612/how-to-remove-the-last-character-from-a-string
            resultingString += "))"; // need two since desired result is (int, (set))
            result.set(resultingString);
            Text finalResult = new Text();

            finalResult.set("("+key.toString()+", ");

            context.write(finalResult, result);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountSet.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "word set");
        job2.setJarByClass(WordCount.class);
        job2.setMapperClass(SwapMapper.class);
        job2.setReducerClass(WordCombiner.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 1 : 2);
    }
}
