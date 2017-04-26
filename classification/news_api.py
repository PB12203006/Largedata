import requests
import json
import sys
reload(sys)
sys.setdefaultencoding('UTF8')
txt_file = "news_sections_abstract2016.txt"
try:
    outfile = open(txt_file, "w")
except IOError as e:
    print("I/O error({0}): {1}".format(e.errno, e.strerror))
# archive
sections = {}
content = {"abstract":1,"snippet":1}
arts = {}

for month in range(1,13):
    response = requests.get('https://api.nytimes.com/svc/archive/v1/2016/'+str(month)+'.json?api-key=5d466e095f4643a98339684082e929e7').json()

# mostpopular + mostviewed
#result = requests.get('https://api.nytimes.com/svc/mostpopular/v2/mostviewed/all-sections/7.json?api-key=5d466e095f4643a98339684082e929e7').json()

#result = requests.get('https://api.nytimes.com/svc/news/v3/content/all/all/240.json?api-key=5d466e095f4643a98339684082e929e7').json()
#print json.dumps(result, indent=4)

#print json.dumps(result, indent=4)
#print result["num_results"]
    results = response["response"]["docs"]

    print len(results)
    for i in range(len(results)):
        if results[i]["abstract"]:
            label = results[i]["section_name"]
            if label and label.decode('utf-8') in ['Arts','U.S.','Business Day'] and results[i]["subsection_name"]:
                #if "MUSIC" in results[i]['des_facet'][-1].decode('utf-8'):
                label = results[i]["subsection_name"].decode('utf-8')
                if label in arts.keys():
                    arts[label] +=1
                else:
                    arts[label] =1
            if label in ['World','Sports','Fashion & Style','Books','Music', \
                        'Television','Movies','Technology','Science','Food','Real Estate','Theater', \
                        'Health','Travel','Education','Your Money','Politics','Economy','Art & Design']:
                try:
                    outfile.write(label+"  "+results[i]["abstract"].decode('utf-8')+"\n")
                    content["abstract"] += 1
                    if label in sections:
                        sections[label] += 1
                    else:
                        sections[label] = 1
                except Exception, e:
                    pass
            #print results[i]["abstract"]
        else:
            label = results[i]["section_name"]
            if label and label.decode('utf-8') in ['Arts','U.S.','Business Day'] and results[i]["subsection_name"]:
                #if "MUSIC" in results[i]['des_facet'][-1].decode('utf-8'):
                label = results[i]["subsection_name"].decode('utf-8')
            if label in ['World','Sports','Fashion & Style','Books','Music', \
                        'Television','Movies','Technology','Science','Food','Real Estate','Theater', \
                        'Health','Travel','Education','Your Money','Politics','Economy','Art & Design']:
                try:
                    outfile.write(label+"  "+results[i]["abstract"].decode('utf-8')+"\n")
                    content["snippet"] += 1
                    if label in sections:
                        sections[label] += 1
                    else:
                        sections[label] = 1
                except Exception, e:
                    pass

sorted_sections = sorted(sections, key=sections.__getitem__)
print json.dumps(content, indent=4)
for x in sorted_sections:
    print (x, sections[x])
print len(sections)
#print json.dumps(sorted_sections, indent=4)
outfile.close()
"""
(u'Multimedia', 1)
(u'Your Taxes', 1)
(u'Learning', 1)
(u'Great Homes & Destinations', 2)
(u'Afternoon Update', 2)
(u'International Home', 3)
(u'Video Games', 3)
(u'Admin', 3)
(None, 3)
(u'Retirement', 4)
(u'membercenter', 4)
(u'Books; Arts', 7)
(u'Giving', 11)
(u'Times Topics', 11)
(u'Sunday Review', 29)
(u'Public Editor', 43)
(u'Mutual Funds', 45)
(u'Entrepreneurship', 46)
(u'Watching', 58)
(u'Podcasts', 60)
(u'Obituaries', 98)
(u'Job Market', 103)
(u'Automobiles', 116)
(u'Election 2016', 117)
(u'Energy & Environment ', 154)
(u'Universal', 167)
(u'International Arts', 183)
(u'Style', 207)
(u'Economy', 250)
(u'Well', 280)
(u'Today\u2019s Paper', 300)
(u'Briefing', 302)
(u'The Learning Network', 334)
(u'Your Money', 357)
(u'Multimedia/Photos', 363)
(u'International Business', 374)
(u'Corrections', 419)
(u'Times Insider', 454)
(u'Crosswords & Games', 529)
(u'Dance', 553)
(u'Blogs', 572)
(u'NYT Now', 708)
(u'Education', 728)
(u'The Upshot', 769)
(u'Arts', 806)
(u'Media', 849)
(u'Travel', 866)
(u'Magazine', 872)
(u'Science', 957)
(u'Health', 1034)
(u'Theater', 1067)
(u'Art & Design', 1146)
(u'Real Estate', 1253)
(u'Food', 1279)
(u'Technology', 1421)
(u'Television', 1522)
(u'Business Day', 1636)
(u'Movies', 1709)
(u'Music', 1809)
(u'T Magazine', 1840)
(u'Books', 1915)
(u'DealBook', 1968)
(u'U.S.', 3277)
(u'Fashion & Style', 3927)
(u'N.Y. / Region', 4384)
(u'Paid Death Notices', 4777)
(u'Politics', 4853)
(u'Sports', 6551)
(u'Opinion', 7153)
(u'World', 7259)
70
['World','Sports','Paid Death Notices','Fashion & Style','Books','Music','Television','Movies','Technology','Science','Food','Real Estate','Theater','Health','Travel','Education','Your Money','Politics','Economy','Art & Design']

"""
"""
(u'Your Money', 156)
(u'Economy', 194)
(u'Travel', 339)
(u'Real Estate', 353)
(u'Food', 519)
(u'Television', 532)
(u'Science', 609)
(u'Education', 669)
(u'Art & Design', 701)
(u'Theater', 724)
(u'Fashion & Style', 750)
(u'Technology', 793)
(u'Health', 930)
(u'Movies', 1015)
(u'Music', 1083)
(u'Books', 1202)
(u'Politics', 3011)
(u'Sports', 4217)
(u'World', 4426)
19
"""
