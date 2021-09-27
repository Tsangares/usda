import json,pprint,sys
pp = pprint.PrettyPrinter(indent=0)

def get_section(data,section):
    if data == []: return []
    for sectionData in data:
        if section.lower().strip() in sectionData['reportSection'].lower().strip():
            return sectionData['results']
    return None

def get_detail_section(data):
    return get_section(data,"Report Detail")

if __name__=="__main__":
    data = json.load(open('/tmp/test.json'))
    pp.pprint(get_detail_section(data)[0])
