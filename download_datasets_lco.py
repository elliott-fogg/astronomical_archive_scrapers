import requests, sys, json, os, datetime
from urllib import urlencode
from os.path import dirname, abspath
from os.path import join as pathjoin


if not os.path.isdir('data'):
    os.mkdir('data')

# Set base data directory
dir_base = pathjoin(dirname(abspath(__file__)),"data","lco")
if not os.path.isdir(dir_base):
    os.mkdir(dir_base)

def create_data_name(param_dict):
    if param_dict['type'] == 'lco':
        data_name = pathjoin("lco",
            "_".join([param_dict['SITEID'],param_dict['TELID'],
                param_dict['start'].split()[0], param_dict['end'].split()[0]]))
    else:
        print "No valid type in param_dict:"
        print param_dict
        return
    return data_name

def list_datasets(select=False):
    datasets = json.load(open("dataset_parameters.txt","r"))
    if not select:
        for paramset in datasets:
            print create_data_name(paramset)
    else:
        numbered_datasets = list(enumerate(datasets))
        max_num = len(numbered_datasets) - 1
        for number, paramset in numbered_datasets:
            print "{}: {}".format(number, create_data_name(paramset))

        while True:
            numselect = raw_input("Select a dataset: ")
            try:
                numselect = int(numselect)
            except ValueError:
                print "Not valid input. No dataset selected." + \
                    "(Input = '{}')".format(numselect)
                return
            if 0 > numselect or numselect > max_num:
                print "Selection unavailable. Please choose a selection between "+\
                    "0 and {}".format(max_num)
                continue
            return datasets[numselect]

def list_downloaded(type=None):
    pass
    # For each type, get list of downloaded datasets by extracting from title,
    # and seeing whether they have been completed or not

def download_data(param_dict,ask_permission=True):
    # Set directory for this dataset
    dir_path = pathjoin(dir_base,
        param_dict['SITEID'] + "_" + param_dict['TELID'] + "_" + \
        param_dict['start'].split()[0] + "_" + param_dict['end'].split()[0]
        )
    if not os.path.isdir(dir_path):
        os.mkdir(dir_path)

    # TODO: Download files into a temporary folder, and then copy them over when
    # the download is completed. Don't erase any existing files until the
    # download is completed.

    # else:
    #     # Data already exists, do we want to overwrite?
    #     yesno = raw_input("Data folder '{}' already exists. Overwrite? [Y/n] ".format(\
    #         dir_path))
    #     if yesno in ("y","Y",""):
    #         file_list = os.listdir(dir_path)
    #         for filename in file_list:
    #             if "datafile_" in filename or "_complete" in filename:
    #                 os.remove(pathjoin(dir_path,filename))
    #         print "Folder '{}' cleared for next download.".format(dir_path)
    #     else:
    #         print "Download aborted."
    #         return

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
    if ask_permission:
        yn_continue = raw_input('Download {} entries? [Y/n] '.format(total_count))
        if yn_continue not in ('','y','Y'):
            print "Download aborted."
            return

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
    # -a --all - Download all available datasets
    # -i --incomplete - Download all incomplete datasets (default?)
    # -s --select - Select datasets for download (default?)

    if '-a' in sys.argv or '--all' in sys.argv:
        permission_required = False
    else:
        permission_required = True

    param_sets = json.load(open("dataset_parameters.txt","r"))

    for param_dict in param_sets:
        download_data(param_dict,permission_required)
