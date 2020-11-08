cat  output/part-r-00000 | tr "\\t" "," >  comparison/output.csv
cat  answer.txt | tr "\\t" "," >  comparison/answer.csv
sort -o comparison/output.csv comparison/output.csv
sort -o comparison/answer.csv comparison/answer.csv