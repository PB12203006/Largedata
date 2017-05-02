'''
Fetch the 4 months' latest news through NY Times API
Write headline/abstract/url in 3 files respectively
'''
import requests
import json
import sys
reload(sys)
sys.setdefaultencoding('UTF8')
txt_file0 = "news_recent_headline.txt"
txt_file1 = "news_recent_abstract.txt"
txt_file2 = "news_recent_url.txt"

months = [4]
try:
    outfile0 = open(txt_file0, "w")
    outfile1 = open(txt_file1, "w")
    outfile2 = open(txt_file2, "w")
except IOError as e:
    print("I/O error({0}): {1}".format(e.errno, e.strerror))

sections = {}
content = {"abstract":1,"snippet":1}
for month in months:
    response = requests.get('https://api.nytimes.com/svc/archive/v1/2017/'+str(month)+'.json?api-key=5d466e095f4643a98339684082e929e7').json()
    results = response["response"]["docs"]
    print len(results)
    for i in range(len(results)):
        if results[i]["abstract"]:
            text = results[i]["abstract"]
        else:
            text = results[i]["snippet"]
        label = results[i]["section_name"]
        if label and label.decode('utf-8') in ['Arts','U.S.','Business Day'] and results[i]["subsection_name"]:
            label = results[i]["subsection_name"].decode('utf-8')
        if label in ['World','Sports','Fashion & Style','Books','Music', \
                    'Television','Movies','Technology','Science','Food','Real Estate','Theater', \
                    'Health','Travel','Education','Your Money','Politics','Economy','Art & Design']:
            try:
                outfile0.write(label+"  "+results[i]["headline"]["main"].decode('utf-8')+"\n")
                outfile1.write(label+"  "+text.decode('utf-8')+"\n")
                outfile2.write(label+"  "+results[i]["web_url"].decode('utf-8')+"\n")
                content["abstract"] += 1
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
outfile0.close()
outfile1.close()
outfile2.close()
