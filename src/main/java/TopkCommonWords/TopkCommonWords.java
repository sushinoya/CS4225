package TopkCommonWords;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class TopkCommonWords {
    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        HashSet<String> stopWords = new HashSet<>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // super.setup(context);
            Configuration conf = context.getConfiguration();
            FileSystem fileSystem = FileSystem.get(conf);
            Path stopWordsFilePath = new Path(conf.get("stopWordsFile"));
            BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(stopWordsFilePath)));

            stopWords.add(""); // Add empty string because it is not a valid word

            String currentWord = reader.readLine();
            while (currentWord != null) {
                this.stopWords.add(currentWord);
                currentWord = reader.readLine();
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // super.map(key, value, context);
            String[] words =value.toString().split(" ");

            for (String word: words) {
                if (!this.stopWords.contains((word))) {
                    context.write(new Text(word), new IntWritable(1));
                }
            }
        }
    }

    public static class WordCountSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalCount = 0;
            for(IntWritable count : values) {
                totalCount += count.get();
            }
            context.write(key, new IntWritable(totalCount));
        }
    }

    public static class MultiInputMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            // super.map(key, value, context);
            context.write(key, value);
        }
    }

    public static class CombineWordCountsReducer extends Reducer<Text, Text, IntWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException, UnsupportedOperationException {
            ArrayList<Integer> valuesList = new ArrayList<>();

            for (Text value: values) {
                valuesList.add(Integer.valueOf(value.toString()));
            }

            if (valuesList.size() < 2) {
                return;
            } else if (valuesList.size() > 2) {
                throw new UnsupportedOperationException("Unable to work with more than 2 values");
            }

            int smallerCount = Math.min(valuesList.get(0), valuesList.get(1));
            context.write(new IntWritable(smallerCount), key);
        }
    }

    public static class TruncateAndSortReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private IntWritable countSoFar = new IntWritable();
        private int K;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            this.K = conf.getInt("K");
        }

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        }
    }


    private static Job createWordCountJob(Path inputPath, Path outputPath, Configuration conf, String jobDesc) throws IOException {
        Job wordCountJob = Job.getInstance(conf, jobDesc);
        wordCountJob.setJarByClass(TopkCommonWords.class);
        wordCountJob.setMapperClass(WordCountMapper.class);
        wordCountJob.setCombinerClass(WordCountSumReducer.class);
        wordCountJob.setReducerClass(WordCountSumReducer.class);

        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);

        wordCountJob.setNumReduceTasks(1);

        FileInputFormat.addInputPath(wordCountJob, inputPath);
        FileOutputFormat.setOutputPath(wordCountJob, outputPath);
        return wordCountJob;
    }

    public static void main(String[] args) {
        Path inputFile1 = new Path(args[0]);
        Path inputFile2 = new Path(args[1]);
        Path stopWordsFile = new Path(args[2]);
        Path outputFile = new Path(args[3]);
        Path wordCountsFile1 = new Path("intermediate_results//file1");
        Path wordCountsFile2 = new Path("intermediate_results//file2");
        Path combinedCountsFile = new Path("intermediate_results//combined");

        System.out.println("Setting up jobs");
        Configuration conf = new Configuration();
        conf.set("stopWordsFile", stopWordsFile.toString());
        conf.setInt("K", 20);


        // Count Words in First File
        try {
            Job countFirstFileWordsJob = TopkCommonWords.createWordCountJob(inputFile1,
                    wordCountsFile1, conf,"Count Words in First File");
            countFirstFileWordsJob.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Count Words in Second File
        try {
            Job countSecondFileWordsJob = TopkCommonWords.createWordCountJob(inputFile2,
                    wordCountsFile2, conf, "Count Words in Second File");
            countSecondFileWordsJob.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Obtain the common words
        try {
            Job combineWordCountsJob = Job.getInstance(conf, "Combine word counts");
            combineWordCountsJob.setJarByClass(TopkCommonWords.class);

            combineWordCountsJob.setReducerClass(CombineWordCountsReducer.class);

            MultipleInputs.addInputPath(combineWordCountsJob, wordCountsFile1, KeyValueTextInputFormat.class, MultiInputMapper.class);
            MultipleInputs.addInputPath(combineWordCountsJob, wordCountsFile2, KeyValueTextInputFormat.class, MultiInputMapper.class);

            combineWordCountsJob.setMapOutputKeyClass(Text.class);
            combineWordCountsJob.setMapOutputValueClass(Text.class);

            combineWordCountsJob.setOutputKeyClass(IntWritable.class);
            combineWordCountsJob.setOutputValueClass(Text.class);
            combineWordCountsJob.setNumReduceTasks(1);

            FileOutputFormat.setOutputPath(combineWordCountsJob, combinedCountsFile);
            combineWordCountsJob.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
