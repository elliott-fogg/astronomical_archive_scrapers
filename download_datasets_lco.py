import requests, sys, json, os, datetime
from urllib import urlencode
from os.path import dirname, abspath
from os.path import join as pathjoin

param_sets = [
    {
        "start": "2016-02-01 00:00",
        "end": "2016-08-01 00:00",
        "public": "true",
        "SITEID": "coj",
        "TELID": "2m0a",
        "limit": "100"
    },
    {
        "start": "2016-02-01 00:00",
        "end": "2016-08-01 00:00",
        "public": "true",
        "SITEID": "ogg",
        "TELID": "2m0a",
        "limit": "100"
    }
]

if not os.path.isdir('data'):
    os.mkdir('data')

# Set base data directory
dir_base = pathjoin(dirname(abspath(__file__)),"data","lco_data")

def download_data(param_dict):
    # Set directory for this dataset
    main_dir = pathjoin(dir_base,
        param_dict['SITEID'] + "_" + param_dict['TELID'] + "_" + \
        param_dict['start'].split()[0] + "_" + param_dict['end'].split()[0]
        )
    if not os.path.isdir(main_dir):
        os.mkdir(main_dir)

    # Set directory for this specific download
    dir_index = 0
    while True:
        dir_path = pathjoin(main_dir,"set_" + str(dir_index))
        if os.path.isdir(dir_path):
            dir_index += 1
        else:
            os.mkdir(dir_path)
            break

    url_base = "https://archive-api.lco.global/frames/?"
    encoded_params = urlencode(param_dict)
    url = url_base + encoded_params

    # Initial Request to check success and entry number
    r = requests.get(url=url)
    if r.status_code != 200:
        print "Request Failed. Status Code: {}; Reason: {}".format(\
            r.status_code, r.reason)
        return

    # Check length of download
    request_data = r.json()
    total_count = request_data['count']
    # yn_continue = raw_input('Download {} entries? [Y/n] '.format(total_count))
    # if yn_continue not in ('','y','Y'):
    #     print "Download aborted."
    #     return

    # Set up loop variables
    try:
        request_limit = int(param_dict['limit'])
    except KeyError:
        request_limit = 100
    file_base = "datafile_{}.json"
    file_index = 0
    counter = 0
    file_entry_limit = 5000
    next_file_count = file_entry_limit
    temp_frame_list = []

    while True:
        sys.stdout.write("\rDownloaded {} / {}".format(counter, total_count))
        sys.stdout.flush()

        if counter >= next_file_count:
            file_path = pathjoin(dir_path,file_base.format(file_index))
            with open(file_path,"w") as openfile:
                openfile.write(json.dumps(temp_frame_list))
            temp_frame_list = []
            next_file_count += file_entry_limit
            file_index += 1

        results = request_data['results']
        for frame in results:
            temp_frame_list.append(frame)

        if request_data['next'] == None:
            break

        next_url = request_data['next']
        counter += request_limit

        r = requests.get(url=next_url)
        if r.status_code != 200:
            print "Request Failed. Status Code: {}; Reason: {}".format(\
                r.status_code, r.reason)
            return

        request_data = r.json()

    # Exited Loop
    file_path = pathjoin(dir_path,file_base.format(file_index))
    with open(file_path,"w") as openfile:
        openfile.write(json.dumps(temp_frame_list))

    completefile_path = pathjoin(dir_path,"_complete")
    with open(completefile_path,"w") as cfile:
        cfile.write(str(datetime.datetime.now()))

    print "\nDownload of {} files comlpeted".format(total_count)

################################################################################

if __name__ == '__main__':
    for param_dict in param_sets:
        download_data(param_dict)
