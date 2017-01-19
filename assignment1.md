Question 1
==========
Pairs
-----
I plan on following the pairs example in class and building my implementation off of that. I will split my implementation into 2 jobs. 1 for counting words and lines and the second for reducing to calculate PMIs. 
The input records will be each line in the file. The intermediate key-value pairs in the first job will simply be Text and LongWritable.
The input records for the second job will be the output from the first job, counts of co-occurrences and words. As suggested in the assignment, the final key output will be a co-occurring pair and the value will be a pair with PMI and co-occurrence count.

Stripes
-------
Much like the pairs problem, I will follow the example in class and build my implementation from there. I will again split my implementation into 2 jobs that perform the same as the pairs implementation.
The input records will be each line in the file. The intermediate key-value pairs in the first job will be text as keys and a map of counts as the values.
The input records for the second job will be the output from the first job, counts of co-occurrences and words. As suggested in the assignment, the final key output will be Text and the value will be a map of co-occurring words and pairs of PMIs and counts.

Question 2
==========
Pairs
-----
10.091 + 32.897 = 42.988 seconds

Ran on UW linux environment

Stripes
-------
8.141 + 24.013 = 32.154 seconds

Ran on UW linux environment

Question 3
==========
Pairs
-----
13.301 + 46.226 = 59.527 seconds

Ran on UW linux environment

Stripes
-------
12.235 + 35.279 = 47.514 seconds

Ran on UW linux environment

Question 4
==========
37917  151668 1140347

Question 5
==========
Highest PMI
-----------
(milford, haven)        (3.6201773, 11)

(anjou, maine)  (3.5953538, 11)

This makes sense as these are low volume words of specific locations that are not likely to occur very often on their own.

Lowest PMI
----------
(you, thou)     (-1.622328, 12)

(of, enter)     (-1.411942, 11)

This makes sense as these words are high volume meaning they will dilute the probability of co-occurring with one another.

Question 6
==========
tears
-----
(tears, are)    (0.19612648, 12)

(tears, for)    (-0.115245804, 13)

(tears, his)    (-0.171579, 10)

death
-----
(death, life)   (0.36071625, 13)

(death, hath)   (-0.0070340387, 13)

(death, or)     (-0.020579664, 15)

Question 7
==========

Question 8
==========
