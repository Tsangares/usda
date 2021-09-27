import requests,json,pprint,logging,csv,time
from process_slug import *
from multiprocessing import Pool

pp = pprint.PrettyPrinter(indent=0) #For nice console output
apikey_filename = ".apikey.json" #Login information for usda.gov
key = json.load(open(apikey_filename)) #Load api keys
auth = (key['apikey'],key['password']) #Set authentication headers

#Gets a list of all the slugs in general.
def get_slugs(commodity=None):
    endpoint = "https://marsapi.ams.usda.gov/services/v1.2/reports/"
    params = {}
    if commodity is not None:
        params = {
            'q': f'commodity={commodity}'
        }
    r = requests.get(endpoint,auth=auth,params=params)
    data = json.loads(r.text)
    pp.pprint(data)
    return [d['slug_id'] for d in data]

#Gets a list of all the markets and their ids.
def get_market_type_ids():
    endpoint = 'https://marsapi.ams.usda.gov/services/v1.2/marketTypes'
    return json.load(requsets.get(endpiont,auth=auth).text)

#Gets a response from a specific market id; returns report slugs.
def get_market_type_slugs(market_type_id):
    endpoint = f'https://marsapi.ams.usda.gov/services/v1.2/marketTypes/{market_type_id}'
    r = requests.get(endpoint,auth=auth)
    data = json.loads(r.text)['results']
    return [d['slug_id'] for d in data]

#Gets the report of a specific slug (slow)
def get_slug(slug,count=0):
    endpoint = f"https://marsapi.ams.usda.gov/services/v1.2/reports/{slug}?q&allSections=true"
    if count >= 5:
        logging.error(f'Failed to get 5 times. {endpoint}')
        return []
    logging.info(f'Collecting {endpoint}')
    start = time.time()
    response = requests.get(endpoint,auth=auth)
    if response.status_code != 200:
        logging.error(f'Failed to get: {response} {endpoint}')
        delay = 5
        logging.warning(f"Retrying in {delay} secconds")
        return get_slug(slug,count+1)
    try:
        data = json.loads(response.text)
    except Exception as e:
        print(e)
        pp.pprint(response.text)
        return []
    duration = time.time() - start
    logging.info(f'Finished ({duration:.02f} sec) collecting {endpoint}')
    return data

#Wrapper for getting slug, strips it to just get report details.
def get_slug_details(slug):
    return get_detail_section(get_slug(slug))

#Command line access
if __name__=="__main__":
    logging.basicConfig(level=logging.INFO)    
    #slugs = get_slugs()
    #markets = get_market_type_ids()
    market_id = 1083 #Grain maket

    #Get every slug of every type of entry.
    slugs = get_market_type_slugs(market_id)

    all_data = []
    #Muliprocess getting all the data
    with Pool(30) as pool:
        all_data = pool.map(get_slug_details,slugs)
        

    #Flatten multiprocessing output.
    output = []
    for data in all_data:
        output+=data
    
    #Write data output
    logging.info("Writing output to json")
    json.dump(output,open(f'usda_deatils_{market_id}.json','w+'),indent=2)
    keys = output[0].keys()
    logging.info("Writing output to csv")    
    with open(f'usda_details_{market_id}.csv','w+',newline='') as output_file:
        dict_writer = csv.DictWriter(output_file,keys)
        dict_writer.writeheader()
        dict_writer.writerows(output)
    print("done.")
