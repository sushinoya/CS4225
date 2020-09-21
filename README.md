# CS4225 Assignment 1


## Task 1
The `WordCountMapper` and `WordCountSumReducer` first work together to produce the word frequency of each file. Stopwords are ignored during this step. The result is stored in `intermediateResults/ file<FileNumber>`
Then `MultiInputMapper` and `CombineWordCountsReducer` combine the frequencies from the two files and keep only the smaller of the two frequencies for each word. The output key is the frequency and the value is the word.
`TruncateAndSortMapper` negates the key for each key, value pair in the previous step. This is so that when Hadoop sorts by key, the words with the biggest counts appear earlier.
`TruncateAndSortReducer` processes the first K key, value pairs and removes the negation from the key in the previous step making it the actual count, word pairs.

## Task 2
There are several MapReduce jobs that I required to achieve the final output. I shall explain them here one by one -

### 1. User Preference Consolidate Job
**Input Format:** (CSV) `userID`, `itemID, itemRating`<br />
**Output Format:** Key: `userID` Value: `item1.id:item1.rating,item2.id:item2.rating...`<br />
**Additional Details:** This was a simple mapping from CSV and grouping ratings by the same user.

### 2. Co-Occurrence Sum Job
**Input Format:** Key: `userID` Value: `item1.id:item1.rating, item2.id:item2.rating...`<br />
**Output Format:** Key: `item1.id item2.id`, Value: `<Items Co-Occurrence count>`<br />
**Additional Details:** In this step, our `CooccurrenceCountMapper` maps a count of 1 for each item pair and `CooccurrenceSumReducer` combines the sum to obtain our Co-occurrence matrix.

### 3. Generate Cooccurrence Matrix Rows Job
**Input Format:** Key: `item1.id item2.id`, Value: `<Items Co-Occurrence count>`<br />
**Output Format:** Key: `item1.id`, Value: `CMRitem2.id:<item1&2 cooccurrence count>,item3.id:<item1&3 cooccurrence count>...`<br />
**Additional Details:** Using our previous output, we generate a list of co-occurrence counts for each item. We prefix this output with `CRM` to identify it when we use the output for Step 5.

### 4. Generate Item Users Ratings Job
**Input Format:** Key: `userID` Value: `item1.id:item1.rating, item2.id:item2.rating...`<br />
**Output Format:** Key: `itemID`, Value: `IURuserA:ratingA,userB:ratingB,userC,ratingC`<br />
**Additional Details:** For every user, we generate a list of his/her ratings in the format defined above. The Mapper writes individual ratings to the context while the Reducer combines them into a big Text delineated by commas. We prefix this output with `IUR` to identify it when we use the output for Step 5.

### 5. CombineUserRatingsAndMatrixRows
**Input A Format:** Key: `item1.id`, Value: `CMRitem2.id:<item1&2 cooccurrence count>,item3.id:<item1&3 cooccurrence count>...`<br />
**Input B Format:** Key: `itemID`, Value: `IURuserA:ratingA,userB:ratingB,userC,ratingC`<br />
**Output Format:** `IURuserA:ratingA,userB:ratingB,userC,ratingC &CMRitem2.id:<item1&2 cooccurrence count>,item3.id:<item1&3 cooccurrence count>...`<br />
**Additional Details:** This step just merges the outputs of Steps 3 and 4 separated by a "&". The thing to note here is that the two outputs which are merged in this step correspond to two rows of the matrices we wish to multiply - 

![](https://i.imgur.com/QsC5ReW.png)

### 6. Multiply Row By Row
**Input Format:** `IURuserA:ratingA,userB:ratingB,userC,ratingC &CMRitem2.id:<item1&2 cooccurrence count>,item3.id:<item1&3 cooccurrence count>...`<br />
**Output Format:** Key: `user.id item.id`, Value: `score`<br />
**Additional Details:** In this step, we multiply each row of the first matrix with each row of the second. The reason why processing row by row works is that the Co-Occurrence Matrix is symmetrical across its diagonal i.e `matrix[100][101] == matrix[101][100]`. For each `user.id` and `item.id` pair, several products (of co-occurrence value and rating) are written to the context by the Mapper. These products are later added together by the Reducer to give us our final result.
To avoid outputting scores for items that user rated, I set one of the values of `user.id`, `item.id` to `<SKIP>`. When for any cell in the final matrix the reducer finds a `<SKIP>`, it aborts summing up the answer for that cell and proceeds to a different cell.
