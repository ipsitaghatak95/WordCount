import re	# to insert string of regular expressions
import pandas as pd	# for dataframes
import queue	# to store data into a queue structure
import threading	#to support multi threading



# Task-1: Data cleaning function
 
def cleanData(file):
    clean_data = []	# creating an empty list
    # Removing the numbers, punctuations  and other unnecessary symbols
    f = re.compile("[a-z]+")
    for line in file:
        if len(line) > 0:
            clean_data.append(f.findall(line.lower()))	# appending the characters into the empty list 
    clean_data = [x for x in clean_data if x != []]
    # list = [item for i in clean_data for item in i]
    return clean_data

# Output will be a list of clean_data

# Task-2: Data split function

# Data Split function
def split(clean_data):
    LinesNumber = []	# creating an empty list
    for line in clean_data:
        LinesNumber.append(line)
    #splitting the data into two files
    Part1 = LinesNumber[0:5000]        # part 1 with first 5000 lines 
    Part2 = LinesNumber[5001:]         # Part 2 with the remaining lines
    splitParts = [Part1, Part2]       
    return splitParts

# Task-3: 

# Mapping with two mapper functions

# Implementing Mapper function for Part 1 with first 5000 lines

def mapper1(splitData, mapper_q1):
    a = [words for i in splitData for words in i]
    map = []
    for i in a:
        word = i
        map.append("%s\t%d" % (word, 1))    # mapping words
    # pushing these mapping words into first queue
    mapper_q1.put(map)

# Implementing Mapper function for Part 2 with the rest lines

def mapper2(splitData, mapper_q2):
    a = [words for i in splitData for words in i]
    map = []
    for i in a:
        word = i
        map.append("%s\t%d" % (word, 1))
    # pushing these mapping words into second queue
    mapper_q2.put(map)

# NOTE: we have used same code for both of the mappers

# Task-4 Sort function

def Sort(map1, map2):
    map = map1 + map2
    map.sort()
    return map


# Task-5 Partition function
    
def Partition(Sorted_data):
    Part1 = []
    Part2 = []
    for m in Sorted_data:
        a = re.search("^[a-m]", m)
        if (a):
            Part1.append(m) # it will have all the tokens starting with letter "a" to "m"
        else:
            Part2.append(m) # it will have other ("n" to "z") letters
    Parts = [Part1, Part2]
    return Parts

# Task-6 Reducer function for letters "a" to "m"

def Reducer1(parts, reducer_q1):
    word_current = None
    count_current = 0
    freq = {}
    for i in parts:
        word, count = i.split('\t', 1)
        count = int(count)
        if word_current == word:
            count_current += count
        else:
            word_current = word
            count_current = count
        freq[word_current] = count_current     # counts  the frequency of a word
    if word_current == word:
        freq[word_current] = count_current
    
    # puahing these tokens into first queue
    return reducer_q1.put(freq)

# Reducer function for Part 2 for letters "n" to "z"
    
def Reducer2(parts, reducer_q2):
    word_current = None
    count_current = 0
    freq = {}
    for i in parts:
        word, count = i.split('\t', 1)
        count = int(count)
        if word_current == word:
            count_current += count
        else:
            word_current = word
            count_current = count
        freq[word_current] = count_current
    if word_current == word:
        freq[word_current] = count_current
    # pushing these elements into second queue
    return reducer_q2.put(freq)

# NOTE: We have used same code for the reducer function

# Main Function - Wrapping all steps together and combining the output of two reducers

def Main(Path):
    # creating queues for two mapper and two reducer functions
    mapper_q1 = queue.Queue()
    mapper_q2 = queue.Queue()
    reducer_q1 = queue.Queue()
    reducer_q2 = queue.Queue()
    # reading the input file from our computer
    file = open(Path, encoding="utf8")
    
    # Calling data cleaning function
    clean_data = cleanData(file)
    
    # calling data split function
    split_data = split(clean_data)
    
    # mapping with two mapper functions
    map1 = threading.Thread(target=mapper1, args=(split_data[0], mapper_q1))
    map2 = threading.Thread(target=mapper2, args=(split_data[1], mapper_q2))
    map1.start()
    map2.start()
    map1.join()
    map2.join()
    map1 = []
    for m in mapper_q1.get():
        map1.append(m)
    map2 = []
    for n in mapper_q2.get():
        map2.append(n)
    Sorted_data = Sort(map1, map2)
    Parts = Partition(Sorted_data)
    
    # Reducing with two reducer function
    Reduce1 = threading.Thread(target=Reducer1, args=(Parts[0], reducer_q1))
    Reduce2 = threading.Thread(target=Reducer2, args=(Parts[1], reducer_q2))
    Reduce1.start()
    Reduce2.start()
    Reduce1.join()
    Reduce2.join()
    Reduce1 = reducer_q1.get()
    Reduce2 = reducer_q2.get()
    Reduce1.update(Reduce2)
    Output = Reduce1
    Output = pd.DataFrame(Output.items(), columns=['Word', 'Count'])    # storing the output into a dataframe called Output
    
    # Mention the path of the Output CSV file below
    Output.to_csv(r'C:\\Users\\IPS\\Documents\\Sem 3\\Big Data\\HW\WordCount\\WordCount_output.csv')     
    return Output     # this will return the output into a csv file


# Mention the path of the input file below

Main(Path='C:\\Users\\IPS\\Documents\\Sem 3\\Big Data\\HW\WordCount\\Test_Data.txt')