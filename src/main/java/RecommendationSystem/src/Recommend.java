package RecommendationSystem.src;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import TopkCommonWords.TopkCommonWords;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityMapper;
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

            context.write(key, new Text("CMR" + itemCountBuilder.toString()));
        }
    }


    public static void runGenerateCooccurrenceMatrixRowsJob(Configuration conf, Path inputPath, Path outputPath) {
        try {
            Job cooccurrenceMatrixRowsJob = Job.getInstance(conf, "Generate Cooccurrence Matrix Rows");
            cooccurrenceMatrixRowsJob.setJarByClass(Recommend.class);
            cooccurrenceMatrixRowsJob.setMapperClass(Recommend.generateCooccurrenceMatrixRowsMapper.class);
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
            context.write(key, new Text("IUR" + userRatingsBuilder.toString()));
        }
    }

    public static void runGenerateItemUsersRatingsJob(Configuration conf, Path inputPath, Path outputPath) {
        try {
            Job generateItemUsersRatingsJob = Job.getInstance(conf, "Generate Item Users Ratings");
            generateItemUsersRatingsJob.setJarByClass(Recommend.class);
            generateItemUsersRatingsJob.setMapperClass(Recommend.generateItemUsersRatingsMapper.class);
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

    public static class MultiInputMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class CombineUserRatingsAndMatrixRowsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException, UnsupportedOperationException {
            String itemUserRatings = null;
            String cooccurrenceMatrixRow = null;
            for (Text value: values) {
                String stringValue = value.toString();
                if (stringValue.startsWith("IUR")) {
                    itemUserRatings = stringValue;
                } else if (stringValue.startsWith("CMR")) {
                    cooccurrenceMatrixRow = stringValue;
                } else {
                    throw new UnsupportedOperationException();
                }
            }
            assert(itemUserRatings != null && cooccurrenceMatrixRow != null); // Sanity check
            context.write(key, new Text(String.format("%s&%s", itemUserRatings, cooccurrenceMatrixRow)));
        }
    }

    public static void runCombineUserRatingsAndMatrixRows(Configuration conf, Path inputA, Path inputB, Path output) {
        try {
            Job combineRatingsAndRowsJob = Job.getInstance(conf, "Combine User Ratings And Matrix Rows");
            combineRatingsAndRowsJob.setJarByClass(Recommend.class);

            combineRatingsAndRowsJob.setReducerClass(Recommend.CombineUserRatingsAndMatrixRowsReducer.class);

            MultipleInputs.addInputPath(combineRatingsAndRowsJob, inputA, KeyValueTextInputFormat.class, Recommend.MultiInputMapper.class);
            MultipleInputs.addInputPath(combineRatingsAndRowsJob, inputB, KeyValueTextInputFormat.class, Recommend.MultiInputMapper.class);
            FileOutputFormat.setOutputPath(combineRatingsAndRowsJob, output);

            combineRatingsAndRowsJob.setMapOutputKeyClass(Text.class);
            combineRatingsAndRowsJob.setMapOutputValueClass(Text.class);

            combineRatingsAndRowsJob.setOutputKeyClass(Text.class);
            combineRatingsAndRowsJob.setOutputValueClass(Text.class);

            combineRatingsAndRowsJob.setNumReduceTasks(1);
            combineRatingsAndRowsJob.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // The cooccurrence matrix is symmetrical along its diagonal. This allows us to do row by row matrix multiplication

    public static class RowMultiplierMapper extends Mapper<Text, Text, Text, DoubleWritable> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String itemID = key.toString();
            String[] matrixRowAndUserScores = value.toString().split("&");
            String userScores = matrixRowAndUserScores[0].replace("IUR", "");
            String matrixRow = matrixRowAndUserScores[1].replace("CMR", "");

            for (String rowElem: matrixRow.split(",")) {
                String rowItemID = rowElem.split(":")[0];
                Integer cooccurrenceCount = Integer.parseInt(rowElem.split(":")[1]);

                for (String userScore : userScores.split(",")) {
                    String userID = userScore.split(":")[0];
                    Double userItemScore = Double.parseDouble(userScore.split(":")[1]);

                    Text newKey = new Text(String.format("%s\t%s", userID, rowItemID));
                    DoubleWritable newValue = new DoubleWritable(cooccurrenceCount * userItemScore);
                    context.write(newKey, newValue);
                }
            }
        }
    }

    public static class RowMultiplierReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double matrix_cell_sum = 0.0;
            for (DoubleWritable product: values) {
                matrix_cell_sum += product.get();
            }
            context.write(key, new DoubleWritable(matrix_cell_sum));
        }
    }

    public static void runMultiplyRowByRow(Configuration conf, Path input, Path output) {
        try {
            Job multiplyRowByRowJob = Job.getInstance(conf, "Multiply Row By Row Job");
            multiplyRowByRowJob.setJarByClass(Recommend.class);
            multiplyRowByRowJob.setMapperClass(Recommend.RowMultiplierMapper.class);
            multiplyRowByRowJob.setReducerClass(Recommend.RowMultiplierReducer.class);
            multiplyRowByRowJob.setInputFormatClass(KeyValueTextInputFormat.class);

            multiplyRowByRowJob.setOutputKeyClass(Text.class);
            multiplyRowByRowJob.setOutputValueClass(DoubleWritable.class);

            multiplyRowByRowJob.setNumReduceTasks(1);

            FileInputFormat.addInputPath(multiplyRowByRowJob, input);
            FileOutputFormat.setOutputPath(multiplyRowByRowJob, output);
            multiplyRowByRowJob.waitForCompletion(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static class FormatOutputMapper extends Mapper<Text, Text, Text, Text> {
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] userIdAndItemId = key.toString().split(" ");
            String userID = userIdAndItemId[0];
            String itemID = userIdAndItemId[1];
            String recommendationScore = value.toString();

            context.write(new Text(userID), new Text(String.format("%s,%s", itemID, recommendationScore)));
        }
    }

    public static class FormatOutputReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, values.iterator().next());
        }
    }

    public static void runFormatOutputJob(Configuration conf, Path inputPath, Path outputPath) {
        try {
            Job formatOutputJob = Job.getInstance(conf, "Format Output");
            formatOutputJob.setJarByClass(Recommend.class);
            formatOutputJob.setMapperClass(Recommend.FormatOutputMapper.class);
            formatOutputJob.setReducerClass(Recommend.FormatOutputReducer.class);
            formatOutputJob.setInputFormatClass(KeyValueTextInputFormat.class);

            formatOutputJob.setOutputKeyClass(Text.class);
            formatOutputJob.setOutputValueClass(Text.class);

            formatOutputJob.setNumReduceTasks(1);

            FileInputFormat.addInputPath(formatOutputJob, inputPath);
            FileOutputFormat.setOutputPath(formatOutputJob, outputPath);
            formatOutputJob.waitForCompletion(true);

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
        Path userRatingsAndMatrixRowsPath = new Path("intermediateResults//userRatingsAndMatrixRows");
        Path userRecommendationScoresPath = new Path("intermediateResults//userRecommendationScores");
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
        // Output: Key: "item1", Value: "CMRitem1:<item1&1 cooccurrence count>,item2:<item1&2 cooccurrence count>..."

        // Input: Key: userid, Value: item1:rating1,item2:rating2,item3,rating3
        Recommend.runGenerateItemUsersRatingsJob(conf, consolidatedUserPrefPath, itemUsersRatingsPath);
        // Output: Key: itemid, Value: "IURuserA:ratingA,userB:ratingB,userC,ratingC"

        // Input: Key: "item1", Value: "CMRitem1:<item1&1 cooccurrence count>,item2:<item1&2 cooccurrence count>..."
        // Input: Key: itemid, Value: "IURuserA:ratingA,userB:ratingB,userC,ratingC"
        Recommend.runCombineUserRatingsAndMatrixRows(conf, cooccurrenceMatrixRowsPath, itemUsersRatingsPath, userRatingsAndMatrixRowsPath);
        // Output: Key: item1, Value: "CMRuserA:ratingA,userB:ratingB&IURitem1:<item1&1 cooccurrence count>,item2:<item1&2 cooccurrence count>..." ,userB:ratingB,userC,ratingC


        // Change delimiter to commas for final output
        Configuration outputConf = new Configuration();
        outputConf.set("mapred.textoutputformat.separator", ",");

        // Input: Key: item1, Value: "CMRuserA:ratingA,userB:ratingB&IURitem1:<item1&1 cooccurrence count>,item2:<item1&2 cooccurrence count>..." ,userB:ratingB,userC,ratingC"
        Recommend.runMultiplyRowByRow(outputConf, userRatingsAndMatrixRowsPath, outputPath);
        // Output: Key: "userid itemid", Value: score

        // Input: Key: "userid itemid", Value: score
//        Recommend.runFormatOutputJob(conf, userRecommendationScoresPath, outputPath);
        // Output: Key: userid, Value: itemid, score

    }
}
