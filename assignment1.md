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
77198  308792 2327792

Question 5
==========
Highest PMI
-----------
(maine, anjou)  (3.6331422, 12)

(anjou, maine)  (3.6331422, 12)

This makes sense as these are low volume words of specific locations that are not likely to occur very often on their own.

Lowest PMI
----------
(thy, you)      (-1.5303967, 11)

(you, thy)      (-1.5303967, 11)

This makes sense as these words are high volume meaning they will dilute the probability of co-occurring with one another.

Question 6
==========
tears
-----
(tears, shed)   (2.1117902, 15)

(tears, salt)   (2.052812, 11)

(tears, eyes)   (1.165167, 23)

death
-----
(death, father's)       (1.120252, 21)

(death, die)    (0.7541594, 18)

(death, life)   (0.7381346, 31)

Question 7
==========
(hockey, defenceman)    (2.4030268, 147)

(hockey, winger)        (2.3863757, 185)

(hockey, goaltender)    (2.2434428, 198)

(hockey, nhl)   (1.9864639, 940)

(hockey, men's) (1.9682738, 84)

Question 8
==========
(data, storage) (1.9796829, 100)

(data, database)        (1.8992721, 97)

(data, disk)    (1.7935462, 67)

(data, stored)  (1.7868547, 65)

(data, processing)      (1.6476576, 57)
