import csv

with open('training.1600000.processed.noemoticon.csv', encoding='latin-1') as csvfile:
  with open('tweets.txt', "w+") as tweetsfile:
    spamreader = csv.reader(csvfile)
    for row in spamreader:
      tweet = row[-1]
      tweetsfile.write("\n" + tweet)