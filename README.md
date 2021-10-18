Christina Stebbins
Big Data
Homework 3

3-1
The top most common letters in books according to Oxford dictionary are E, A, R, I, O, T, N, and S. In my case, I read t,a,T,A,c,g,C,G,e,N,o,n,I,s,r as the top 16. My data differentiated between capitals and lowercase. Additionally, the data above was taken from all 240,000 entries in the OED, whereas this data was taken from over 60,000 books. A big difference here is that the data for the dictionary does not take into account the usage of the words, while our data and results do by scanning books. However, the usage of letters and words in books is not reflective of real life. Another comparison is in passwords. Wikipedia (password strength entry) claims that the most common letters are a, e, o and r, a combination which does not match the oxford dictionary nor my results, likely because password creation has different patterns than books.

***Hive Query Below***

CREATE TABLE cstebbins_letter_count_1 AS 
    SELECT letter, count(1) AS count FROM 
        (SELECT explode(split
            (regexp_replace
                (line, '[^a-zA-Z0-9\u00E0-\u00FC ]+', ''),
            ''))  
            AS letter FROM cstebbins_doc_2) w 
        GROUP BY letter ORDER BY letter;

Located at cstebbins_letter_count_1

***Code***

Located at /tmp/cstebbins-lettercount-1


3-2
IN PROGRESS

3-3
One problem in our manner of set up is our intake of data. Big Data generally follows the 3 (or more) V’s, dealing with data velocity, volume, and variability. The way we handled our data was not the big data vibe in that it did not take the V’s, in this case especially volume, into account. Instead, all the data was uploaded to one single node. In a big data scenario, the data should be distributed and replicated to at least 3 nodes, with data chunks split up for storage on more than one machine. To fix this, I would, as described above, put the data through a streaming process to storage on multiple nodes with replication.

