package RecommendationSystem.src;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringJoiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class Recommend {
    /***************************************************************
     *   PART 1: Compute the similarities between items.
     ***************************************************************/

    public static class UserPrefConsolidateMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            String userID = tokens[0];
            String itemID = tokens[1];
            String rating = tokens[2];
            String itemAndRating = String.format("%s:%s", itemID, rating);
            context.write(new Text(userID), new Text(itemAndRating));
        }
    }

    public static class UserPrefConsolidateReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringJoiner itemRatingBuilder = new StringJoiner(",");
            for (Text itemRatingPair: values) {
                itemRatingBuilder.add(itemRatingPair.toString());
            }
            context.write(key, new Text(itemRatingBuilder.toString()));
        }
    }

    public static void runUserPrefConsolidateJob(Configuration conf, Path inputPath, Path outputPath) {
        try {
            Job userPrefConsolidateJob = Job.getInstance(conf, "Consolidate User Preferences");
            userPrefConsolidateJob.setJarByClass(Recommend.class);
            userPrefConsolidateJob.setMapperClass(Recommend.UserPrefConsolidateMapper.class);
            userPrefConsolidateJob.setCombinerClass(Recommend.UserPrefConsolidateReducer.class);
            userPrefConsolidateJob.setReducerClass(Recommend.UserPrefConsolidateReducer.class);

            userPrefConsolidateJob.setOutputKeyClass(Text.class);
            userPrefConsolidateJob.setOutputValueClass(Text.class);

            userPrefConsolidateJob.setNumReduceTasks(1);

            FileInputFormat.addInputPath(userPrefConsolidateJob, inputPath);
            FileOutputFormat.setOutputPath(userPrefConsolidateJob, outputPath);
            userPrefConsolidateJob.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static class CooccurrenceCountMapper extends Mapper<Text, Text, Text, IntWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] itemRatingPairs = value.toString().split(",");

            // Populate all items rated by a user
            ArrayList<String> items = new ArrayList<>();
            for (String itemRatingPair: itemRatingPairs) {
                String item = itemRatingPair.split(":")[0];
                items.add(item);
            }

            // Save the pairs of items which a user rated
            for (int i = 0; i < items.size(); i++) {
                for (int j = i + 1; j < items.size(); j++) {
                    String itemsPairIdentifier = String.format("%s %s", items.get(i), items.get(j));
                    context.write(new Text(itemsPairIdentifier), new IntWritable(1));
                }
            }
        }
    }

    public static class CooccurrenceSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> countsIterator = values.iterator();
            // Sum up all the values for each item pair
            int totalCount = 0;
            while (countsIterator.hasNext()) {
                totalCount += countsIterator.next().get();
            }
            context.write(key, new IntWritable(totalCount));
        }
    }

    public static void runCooccurrenceSumJob(Configuration conf, Path inputPath, Path outputPath) {
        try {
            Job cooccuerenceSumJob = Job.getInstance(conf, "Construct Cooccurrence Matrix");
            cooccuerenceSumJob.setJarByClass(Recommend.class);
            cooccuerenceSumJob.setMapperClass(Recommend.CooccurrenceCountMapper.class);
            cooccuerenceSumJob.setCombinerClass(Recommend.CooccurrenceSumReducer.class);
            cooccuerenceSumJob.setReducerClass(Recommend.CooccurrenceSumReducer.class);
            cooccuerenceSumJob.setInputFormatClass(KeyValueTextInputFormat.class);

            cooccuerenceSumJob.setOutputKeyClass(Text.class);
            cooccuerenceSumJob.setOutputValueClass(IntWritable.class);

            cooccuerenceSumJob.setNumReduceTasks(1);

            FileInputFormat.addInputPath(cooccuerenceSumJob, inputPath);
            FileOutputFormat.setOutputPath(cooccuerenceSumJob, outputPath);
            cooccuerenceSumJob.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /***************************************************************
     *   PART 2: Predict the recommendation scores for every user
     ***************************************************************/

    public static class generateCooccurrenceMatrixRowsMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String itemA = key.toString().split(" ")[0];
            String itemB = key.toString().split(" ")[1];
            String count = value.toString();

            context.write(new Text(itemA), new Text(String.format("%s:%s", itemB, count)));
            context.write(new Text(itemB), new Text(String.format("%s:%s", itemA, count)));
        }
    }

    public static class generateCooccurrenceMatrixRowsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringJoiner itemCountBuilder = new StringJoiner(",");
            for (Text itemCountPair: values) {
                itemCountBuilder.add(itemCountPair.toString());
            }
            context.write(key, new Text(itemCountBuilder.toString()));
        }
    }


    public static void runGenerateCooccurrenceMatrixRowsJob(Configuration conf, Path inputPath, Path outputPath) {
        try {
            Job cooccurrenceMatrixRowsJob = Job.getInstance(conf, "Generate Cooccurrence Matrix Rows");
            cooccurrenceMatrixRowsJob.setJarByClass(Recommend.class);
            cooccurrenceMatrixRowsJob.setMapperClass(Recommend.generateCooccurrenceMatrixRowsMapper.class);
            cooccurrenceMatrixRowsJob.setCombinerClass(Recommend.generateCooccurrenceMatrixRowsReducer.class);
            cooccurrenceMatrixRowsJob.setReducerClass(Recommend.generateCooccurrenceMatrixRowsReducer.class);
            cooccurrenceMatrixRowsJob.setInputFormatClass(KeyValueTextInputFormat.class);

            cooccurrenceMatrixRowsJob.setOutputKeyClass(Text.class);
            cooccurrenceMatrixRowsJob.setOutputValueClass(Text.class);

            cooccurrenceMatrixRowsJob.setNumReduceTasks(1);

            FileInputFormat.addInputPath(cooccurrenceMatrixRowsJob, inputPath);
            FileOutputFormat.setOutputPath(cooccurrenceMatrixRowsJob, outputPath);
            cooccurrenceMatrixRowsJob.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class generateItemUsersRatingsMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String userid = key.toString();
            String[] itemRatingPairs = value.toString().split(",");

            for (String itemRatingPair: itemRatingPairs) {
                String item = itemRatingPair.split(":")[0];
                String rating = itemRatingPair.split(":")[1];
                context.write(new Text(item), new Text(new Text(String.format("%s:%s", userid, rating))));
            }
        }
    }

    public static class generateItemUsersRatingsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringJoiner userRatingsBuilder = new StringJoiner(",");
            for (Text userRatingPair: values) {
                userRatingsBuilder.add(userRatingPair.toString());
            }
            context.write(key, new Text(userRatingsBuilder.toString()));
        }
    }

    public static void runGenerateItemUsersRatingsJob(Configuration conf, Path inputPath, Path outputPath) {
        try {
            Job generateItemUsersRatingsJob = Job.getInstance(conf, "Generate Item Users Ratings");
            generateItemUsersRatingsJob.setJarByClass(Recommend.class);
            generateItemUsersRatingsJob.setMapperClass(Recommend.generateItemUsersRatingsMapper.class);
            generateItemUsersRatingsJob.setCombinerClass(Recommend.generateItemUsersRatingsReducer.class);
            generateItemUsersRatingsJob.setReducerClass(Recommend.generateItemUsersRatingsReducer.class);
            generateItemUsersRatingsJob.setInputFormatClass(KeyValueTextInputFormat.class);

            generateItemUsersRatingsJob.setOutputKeyClass(Text.class);
            generateItemUsersRatingsJob.setOutputValueClass(Text.class);

            generateItemUsersRatingsJob.setNumReduceTasks(1);

            FileInputFormat.addInputPath(generateItemUsersRatingsJob, inputPath);
            FileOutputFormat.setOutputPath(generateItemUsersRatingsJob, outputPath);
            generateItemUsersRatingsJob.waitForCompletion(true);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        Path inputPath = new Path(args[0]);
        Path consolidatedUserPrefPath = new Path("intermediateResults//consolidatedUserPref");
        Path cooccurrenceSumPath = new Path("intermediateResults//cooccurrenceSum");
        Path cooccurrenceMatrixRowsPath = new Path("intermediateResults//cooccurrenceMatrixRows");
        Path itemUsersRatingsPath = new Path("intermediateResults//itemUsersRatingsRows");
        Path outputPath = new Path(args[1]);
        Configuration conf = new Configuration();

        // Input (CSV): userid,itemid,rating
        Recommend.runUserPrefConsolidateJob(conf, inputPath, consolidatedUserPrefPath);
        // Output: Key: userid, Value: item1:rating1,item2:rating2,item3,rating3

        // Input: Key: userid, Value: item1:rating1,item2:rating2,item3,rating3
        Recommend.runCooccurrenceSumJob(conf, consolidatedUserPrefPath, cooccurrenceSumPath);
        // Output: Key: "item1 item2", Value: <Co-occurrence Count>

        // Input: Key: "item1 item2", Value: <Co-occurrence Count>
        Recommend.runGenerateCooccurrenceMatrixRowsJob(conf, cooccurrenceSumPath, cooccurrenceMatrixRowsPath);
        // Output: Key: "item1", Value: "item1:<item1&1 cooccurrence count>,item2:<item1&2 cooccurrence count>..."

        // Input: Key: userid, Value: item1:rating1,item2:rating2,item3,rating3
        Recommend.runGenerateItemUsersRatingsJob(conf, consolidatedUserPrefPath, itemUsersRatingsPath);
        // Output: Key: itemid, Value: userA:ratingA,userB:ratingB,userC,ratingC

    }
}
