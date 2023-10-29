import requests
import sys
import json
import os
import datetime
import zipfile
import urllib
import shutil
import argparse
from os.path import join as pathjoin

DATA_DIR = "data"
OLD_DATA_DIR = "old_data"
URL_BASE = "https://archive-api.lco.global/frames/?"

FILE_ENTRY_LIMIT = 5000
REQUEST_ENTRY_LIMIT = 100
FILE_BASE = "datafile_{}.json"


def zip_old_data(clear=False):
    """Copies all existing current data into a zipfile, then clears the 'data/'
    directory."""

    # Get list of all files to copy across
    filelist = []
    for root, dirs, files in os.walk(DATA_DIR):
        for file in files:
            filelist.append([pathjoin(root, file),
                             os.path.relpath(pathjoin(root, file), DATA_DIR)])

    # If no files exist, abort zipping
    if len(filelist) == 0:
        print("No files to zip. Skipping.")
        return

    # Index name of new zip file
    current_index = 0
    while True:
        current_name = f"old_data_{current_index}.zip"
        current_path = pathjoin(OLD_DATA_DIR, current_name)
        if os.path.isfile(current_path):
            current_index += 1
        else:
            break

    # Move contents of data directory to zipfile
    print(f"Zipping existing data into '{current_name}'...")
    with zipfile.ZipFile(current_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(DATA_DIR):
            for file in files:
                filepath = pathjoin(root, file)
                zipf.write(filepath, os.path.relpath(filepath, DATA_DIR))

    # If specified, clear data directory
    if clear:
        for dirname in os.listdir(DATA_DIR):
            shutil.rmtree(pathjoin(DATA_DIR, dirname))


def create_data_name(param_dict):
    data_name = "_".join([param_dict["SITEID"],
                         param_dict["TELID"],
                         param_dict["start"].split()[0],
                         param_dict["end"].split()[0]])
    return data_name


def check_dataset_size(param_dict):
    param_dict['limit'] = 1
    r = request_from_param_dict(param_dict)
    total_count = r.json()["count"]
    return total_count


def dataset_status(paramsets):
    for param_dict in paramsets:
        total_count = check_dataset_size(param_dict)
        data_name = create_data_name(param_dict)
        if os.path.isdir(pathjoin(DATA_DIR, data_name)):
            if os.path.isfile(pathjoin(DATA_DIR, data_name, "_complete")):
                status = "Complete"
            else:
                file_count = len([f for f in \
                                  os.listdir(pathjoin(DATA_DIR, data_name)) \
                                  if f.startswith("datafile")])
                entry_count = file_count * FILE_ENTRY_LIMIT
                complete_percent = round(entry_count / total_count * 100)
                status = f"Incomplete ({complete_percent}%)"
        else:
            status = "Not Started"

        

        print(f"{data_name} [{total_count}] - {status}")


def list_param_files():
    for f in os.listdir():
        if f.split(".")[-1] == "json":
            print(f)


def request_from_param_dict(param_dict):
    encoded_params = urllib.parse.urlencode(param_dict)
    url = URL_BASE + encoded_params
    r = requests.get(url=url)
    if r.status_code != 200:
        print(f"Request failed: {r.status_code}\nReason:\n{r.reason}\n")
    else:
        return r


def ask_permissions(param_dict_list):
    selected_param_dicts = []
    selected_description = []

    for param_dict in param_dict_list:
        data_name = create_data_name(param_dict)

        # Skip if complete
        if os.path.isfile(pathjoin(DATA_DIR, data_name, "_complete")):
            print(f"{data_name} - complete.")
            continue

        yn1 = input(f"{data_name} - incomplete. Check size? [y/N]")
        if yn1 not in ("y", "Y"):
            continue

        total_count = check_dataset_size(param_dict)

        yn = input(f"{total_count} entries. Flag for download? [Y/n]")
        if yn in ("", "Y", "y"):
            selected_param_dicts.append(param_dict)
            selected_description.append(f"{data_name} [{total_count} entries]")

    if len(selected_description) > 0:
        print("\nSelected datasets to download:")
        for desc in selected_description:
            print(desc)
    else:
        print("No datasets selected for download.")

    return selected_param_dicts


def determine_start_index(data_name):
    file_index = 0
    while True:
        filename = FILE_BASE.format(file_index)
        if os.path.isfile(pathjoin(DATA_DIR, data_name, filename)):
            file_index += 1
        else:
            break
    return file_index


def download_data(param_dict):
    data_name = create_data_name(param_dict)
    dir_path = pathjoin(DATA_DIR, data_name)
    os.makedirs(dir_path, exist_ok=True)

    file_index = determine_start_index(data_name)
    offset = file_index * FILE_ENTRY_LIMIT

    param_dict['limit'] = REQUEST_ENTRY_LIMIT
    param_dict['offset'] = offset

    r = request_from_param_dict(param_dict)
    request_data = r.json()
    total_count = request_data['count']

    counter = offset
    current_counter = 0
    temp_frame_list = []

    print(f"Downloading {data_name}...")

    while True:
        sys.stdout.write("\rDownloaded {} / {} ({} files)".format(counter,
                                                                  total_count,
                                                                  file_index))
        sys.stdout.flush()

        if current_counter >= FILE_ENTRY_LIMIT:
            file_path = pathjoin(dir_path, FILE_BASE.format(file_index))
            with open(file_path,"w") as openfile:
                openfile.write(json.dumps(temp_frame_list))
            temp_frame_list = []
            current_counter = 0
            file_index += 1

        results = request_data['results']
        for frame in results:
            temp_frame_list.append(frame)

        if request_data['next'] == None:
            break

        next_url = request_data['next']
        counter += REQUEST_ENTRY_LIMIT
        current_counter += REQUEST_ENTRY_LIMIT

        r = requests.get(url=next_url)
        if r.status_code != 200:
            print(f"Request failed - {r.status_code}\nReason:\n{r.reason}\n")
            return
        request_data = r.json()

    # Exited Loop
    file_path = pathjoin(dir_path, FILE_BASE.format(file_index))
    with open(file_path,"w") as openfile:
        openfile.write(json.dumps(temp_frame_list))

    completefile_path = pathjoin(dir_path,"_complete")
    with open(completefile_path,"w") as cfile:
        cfile.write(str(datetime.datetime.now()))

    print("\nDownload of {} files completed".format(total_count))

### Arg Parser #################################################################

def parse_args(cl_args):
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset_parameters", action="store", nargs="?",
                        default="params_2m_2015-2017.json",
                        help="""Specify a dataset_parameter file to be used.
                        Must be a JSON file. Defaults to
                        'dataset_parameters.json' if not specified.""")
    parser.add_argument("-a", "--all", action="store_true",
                        help="Download all incomplete datasets without asking.")
    parser.add_argument("-z", "--zip", action="store_true",
                        help="Zip existing data and clear the data folder.")
    parser.add_argument("-i", "--info", action="store_true",
                        help="List the download status of each dataset.")
    parser.add_argument("-l", "--list", action="store_true",
                           help="List all available parameter files.")
    parser.add_argument("-n", "--next", nargs="?", const=1, type=int,
                        help="Initiate the next N downloads.")

    args = parser.parse_args(cl_args)

    return args

################################################################################

if __name__ == '__main__':
    # Ensure data directories exist
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(OLD_DATA_DIR, exist_ok=True)

    args = parse_args(sys.argv[1:])

    if args.list:
        list_param_files()
        sys.exit()

    param_sets = json.load(open(args.dataset_parameters, "r"))

    if args.info:
        dataset_status(param_sets)
        sys.exit()

    if args.zip:
        zip_old_data(True)

    # Check which datasets are not completed yet
    incomplete_datasets = []
    for param_dict in param_sets:
        data_name = create_data_name(param_dict)
        if os.path.isfile(pathjoin(DATA_DIR, data_name, "_complete")):
            print(f"{data_name} - Complete")
        else:
            incomplete_datasets.append(param_dict)

    if args.next:
        incomplete_datasets = incomplete_datasets[:args.next]

    if args.all or args.next:
        dataset_status(param_sets)
    else:
        incomplete_datasets = ask_permissions(incomplete_datasets)

    for param_dict in incomplete_datasets:
        download_data(param_dict)
