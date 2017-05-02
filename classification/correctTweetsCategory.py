"""
Collect user feedback of correct Tweets category
Storing the labels and data to compare performance of multiclass perceptron models with different parameters.

by Yilan Ji

"""
import json
dictionary = {'Art & Design':0,'World':1,'Sports':2,'Fashion & Style':3,'Books':4,'Music':5, \
            'Television':6,'Movies':7,'Technology':8,'Science':9,'Food':10,'Real Estate':11,'Theater':12, \
            'Health':13,'Travel':14,'Education':15,'Your Money':16,'Politics':17,'Economy':18}
category = ['Art & Design','World','Sports','Fashion & Style','Books','Music', \
            'Television','Movies','Technology','Science','Food','Real Estate','Theater', \
            'Health','Travel','Education','Your Money','Politics','Economy']

infile = open("data/tweetsPredictCategory.txt", "r")
outfile = open("data/tweetsCorrectCategory.txt", "w")
outfileind = open("data/tweetsCorrectIndex.txt", "w")
outfilepredict = open("data/tweetsPredictCategory0.txt", "w")
lines = infile.readlines()
count = 0
for i in range(len(lines)):
    line = lines[i]
    label = line.split("    ")[0]
    sentence = " ".join(line.split("    ")[1:])
    print "TWEET TEXT:\n", sentence
    print "Predict Label:", label
    print "\nDictionary of index is:", json.dumps(dictionary,indent=4)
    trueLabel = raw_input("True Label index:")
    try:
        trueLabel = int(trueLabel)
    except Exception,e:
        print "Give me an integer!!!!!!\nOne More Time:\n"
        trueLabel = int(raw_input("True Label index:"))
    print "\n"
    delete = 'n'
    while trueLabel not in range(19) and trueLabel!=-1 and delete=='n':
        print "\n\n\n\n\n\nPLEASE input a integer in range(19)!!!!!!\n\n\n\n\n\n"
        print "WRONG INFOR? delete tweet?(y/n)"
        if raw_input("delete:")=='y':
            delete='y'
            print "DELETING..............\n\n\n\n"
        else:
            trueLabel = int(raw_input("\nTrue Label index:"))
    if delete=='n':
        if trueLabel==-1:
            outfilepredict.write(label+"    "+sentence)
            outfile.write("Others"+"    "+sentence)
            outfileind.write(str(trueLabel)+"    "+sentence)
            print "\nWRITE to file complete :)\n"
        elif trueLabel in range(19):
            outfilepredict.write(label+"    "+sentence)
            outfile.write(category[trueLabel]+"    "+sentence)
            outfileind.write(str(trueLabel)+"    "+sentence)
            print "\nWRITE to file complete :)\n"
        count += 1
        print "\ncount",count,"\n\n"

infile.close()
outfile.close()
outfileind.close()
outfilepredict.close()
