import requests, json, os, sys, datetime, time
import urllib
from os.path import join as pathjoin
from dateutil.relativedelta import relativedelta
import multiprocessing as mp
import timeit

# A function to download all data of observations that is publicly available
# from the LCO Archives
archive_website_url = "https://archive.lco.global/"
archive_api_url_base = "https://archive-api.lco.global/frames?"
date_format = "%Y-%m-%d"
entry_limit_per_file = 10000
SECONDS_PER_UPDATE = 5

# Create containing folders
dir_base=pathjoin(os.path.dirname(os.path.abspath(__file__)),"data","lco")
os.makedirs(dir_base, exist_ok=True)

def get_siteids():
    from selenium import webdriver
    import time
    
    wdpath = "webdrivers/chromedriver.exe"
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    browser = webdriver.Chrome(executable_path=wdpath, options=chrome_options)
    
    browser.get("https://archive.lco.global/")

    siteid_options = []
    count = 0
    while True:
        siteid_select = browser.find_element_by_id("siteid")
        siteid_options = [option for option in 
                          siteid_select.find_elements_by_tag_name("option")]
        if len(siteid_options) < 4:
            if count >= 10:
                print("Not enough siteid options could be found.")
                break
            else:
                count += 1
                print("Waiting...")
                time.sleep(3)
            
        else:
            print("Enough siteid options have been found!")
            break
    
    
    siteids = [element.get_attribute("value") for element in siteid_options]
    browser.close()
    return siteids

def count_archive_entries(params):
    url_base = archive_api_url_base
    encoded_params = urllib.parse.urlencode(params)
    url = url_base + encoded_params
    r = requests.get(url=url)
    if not r:
        print("Request Failed. Status Code: {}; Reason: {}; URL: {}".format(\
              r.status_code, r.reason, url))
        return
    
    request_data = r.json()
    entry_count = request_data['count']
    return entry_count

def count_siteid_entries(params):
    # Get entries for each siteid
    siteids = get_siteids()[1:]
    print(siteids)
    
    sum_siteid_entries = 0
    for sid in siteids:
        t_params = params
        t_params['SITEID'] = sid
        entry_count = count_archive_entries(t_params)
        print(sid, ":", entry_count)
        sum_siteid_entries += entry_count

    return sum_siteid_entries

def count_date_entries(params):
    # Get entries for each year
    total_start_dt = datetime.date(2014, 5, 1)
    total_end_dt = datetime.date.today()
    query_start_dt = total_start_dt
    sum_date_entries = 0
    
    while query_start_dt < total_end_dt:
        query_end_dt = query_start_dt + relativedelta(months=3)
        if query_end_dt >= total_end_dt:
            query_end_dt = total_end_dt
        
        t_params = params
        t_params['start'] = query_start_dt.strftime(date_format) + " 00:00"
        t_params['end'] = query_end_dt.strftime(date_format) + " 23:59"
        
        entry_count = count_archive_entries(t_params)
        print(query_start_dt, " - ", query_end_dt, ":", entry_count)
        sum_date_entries += entry_count
        
        query_start_dt = query_end_dt

def check_parameter_sizes():
    # Get the total number of entries in the Archives
    params = {
            "start": "2014-05-01 00:00",
            "end": datetime.datetime.now().strftime(date_format) + " 23:59",
            "limit": 100
            }
    
    total_entries = count_archive_entries(params)
    #sum_siteid_entries = count_siteid_entries(params)
    sum_date_entries = count_date_entries(params)
    
    # Print Results
    print("Total Entries:", total_entries)
    print("SiteID Entries:", sum_siteid_entries, 
          "; Difference:", total_entries - sum_siteid_entries)
    print("DateRange Entries:", sum_date_entries,
          "; Difference:", total_entries - sum_date_entries)
 
def mp_download_frames_between_dates(date_range, queue, name):
    start_dt, end_dt = date_range
    file_name_base = start_dt.strftime("%Y-%m") + "_" + end_dt.strftime("%Y-%m") + "_{}.json"
    url_base = archive_api_url_base
    params = {
        "start": start_dt.strftime(date_format),
        "end": end_dt.strftime(date_format),
        "limit": 100
    }
    encoded_params = urllib.parse.urlencode(params)
    start_url = url_base + encoded_params

    next_url = start_url
    entry_count = 0
    file_index = 0
    temp_frame_list = []

    while True:
        r = requests.get(url=next_url)

        if r.status_code != 200:
            # Log this somehow
            pass

        request_data = r.json()
        results = request_data['results']

        temp_frame_list += request_data['results']
        entry_count += len(results)

        if len(temp_frame_list) >= entry_limit_per_file:
            file_name = file_name_base.format(file_index)
            file_path = os.path.join(dir_base, file_name)
            json.dump(temp_frame_list, open(file_path, "w"))
            file_index += 1

        # Output information to the Queue
        queue.put( (name, entry_count, True) )

        if ("next" in request_data) and (request_data["next"] != None):
            next_url = request_data['next']
        else:
            break

    # Exited loop, save remaining entries to file
    file_name = file_name_base.format(file_index)
    file_path = os.path.join(dir_base, file_name)
    json.dump(data, open(file_path, "w"))

    # End the subprocesses
    queue.put( (name, entry_count, False) )

# def update_process_info(process_entry_counts, active_processes,
#                         total_entries, start_time, timer_counter):
#     completed_entries = sum(process_entry_counts)
#     complete_percentage = round(completed_entries / total_entries * 100)

#     active_process_count = sum(active_processes)

#     current_time = timeit.default_timer()

#     elapsed_time = round(current_time - start_time)

#     predicted_time = round(total_entries / completed_entries * elapsed_time)

#     if timer_counter % 30 == 0:




#     text = "\r{}% complete - {}/{} entries. Elapsed: {}s; Predicted: {}; Active Processes:{}"


#     text = "\r{}% complete - {} entries; Remaining Processes: ".format(percentage, total

#     active_process_strings = "".join(["{}, ".format(i) for i in range(len(active_processes))
#         if active_processes[i]])

#     text += active_process_strings[:-2]

#     sys.stdout.write(text)
#     sys.stdout.flush()

# def control_download_all_files():
#     # Check for completion_file
#     completefile_path = os.path.join(dir_base, "_complete")
#     if os.path.isfile(completefile_path):
#         print("Files apparently already downloaded. Aborting.")
#         return

#     # Check how many total files to download:
#     r = requests.get(archive_api_url_base)
#     request_data = r.json()
#     total_entries = request_data['count']
#     print("Total Entries to download: {}".format(total_entries))


#     # Generate date ranges
#     total_start_dt = datetime.date(2014, 5, 1)
#     total_end_dt = datetime.date.today()
#     dt_pairs = []
#     start_dt = total_start_dt
    
#     while start_dt < total_end_dt:
#         end_dt = start_dt + relativedelta(months=3)
        
#         if end_dt > total_end_dt:
#             end_dt = total_end_dt

#         dt_pairs.append((start_dt, end_dt))
#         start_dt = end_dt

#     # Set up for Parallel Processing
#     queue = mp.Queue()

#     try:
#         # SET UP A QUEUE TO DOWNLOAD THE FILES FOR EACH DATE RANGE
#         processes = [mp.Process(target=mp_download_frames_between_dates,
#                                 args=(dt_pairs[i], queue, i)) for \
#                                 i in range(len(dt_pairs))]

#         process_entry_counts = [0 for p in processes]
#         active_processes = [True for p in processes]

#         for p in processes:
#             p.start()

#         controller = Controller(processes)
#         timer_counter = 0
#         start_time = timeit.default_timer()

#         while any(active_processes):
            
#             text_changed = False
#             while not queue.empty():
#                 text_changed = True
#                 name, entry_count, active = queue.get()
#                 process_entry_counts[name] = entry_count
#                 active_processes[name] = active

#             update_process_info(process_entry_counts, active_processes, 
#                                 total_entries, start_time)

#             time.sleep(1)

#         with open(completefile_path, "w") as cfile:
#             cfile.write(str(datetime.datetime.now()))

#     except KeyboardInterrupt:
#         for p in processes:
#             p.kill()
#         print("Download aborted. Processes terminated.")

def control_download_all_files():
    # Check for completion_file
    completefile_path = os.path.join(dir_base, "_complete")
    if os.path.isfile(completefile_path):
        print("Files apparently already downloaded. Aborting.")
        return

    # Check how many total files to download:
    r = requests.get(archive_api_url_base)
    request_data = r.json()
    total_entries = request_data['count']
    print("Total Entries to download: {}".format(total_entries))


    # Generate date ranges
    total_start_dt = datetime.date(2014, 5, 1)
    total_end_dt = datetime.date.today()
    dt_pairs = []
    start_dt = total_start_dt
    
    while start_dt < total_end_dt:
        end_dt = start_dt + relativedelta(months=3)
        
        if end_dt > total_end_dt:
            end_dt = total_end_dt

        dt_pairs.append((start_dt, end_dt))
        start_dt = end_dt

    # Set up for Parallel Processing
    queue = mp.Queue()

    try:
        # SET UP A QUEUE TO DOWNLOAD THE FILES FOR EACH DATE RANGE
        processes = [mp.Process(target=mp_download_frames_between_dates,
                                args=(dt_pairs[i], queue, i)) for \
                                i in range(len(dt_pairs))]

        controller = Controller(processes, total_entries, queue)
        controller.start_processes()

        while controller.download_is_active():
            controller.check_progress()
            time.sleep(1)

        with open(completefile_path, "w") as cfile:
            cfile.write(str(datetime.datetime.now()))

    except KeyboardInterrupt:
        controller.kill_processes()

class Controller():
    def __init__(self, processes, total_entries, queue):
        self.processes = processes
        self.active_processes = [True for process in self.processes]
        self.process_entry_counts = [0 for process in self.processes]
        self.start_time = timeit.default_timer()
        self.elapsed_time = 0
        self.predicted_time = None
        self.total_entries = total_entries
        self.timer_counter = 0
        self.queue = queue

    def start_processes(self):
        for p in self.processes:
            p.start()

    def kill_processes(self):
        for p in self.processes:
            p.kill()
        print("\nDownload aborted. Processes terminated.")

    def update_entry_counts(self):
        change = False
        while not self.queue.empty():
            change = True
            name, entry_count, active = self.queue.get()
            self.process_entry_counts[name] = entry_count
            self.active_processes[name] = active
        return change

    def format_to_shortest_time(self, time_in_seconds):
        hours = int(time_in_seconds / 3600)
        minutes = int(time_in_seconds / 60) - (60 * hours)
        seconds = int(time_in_seconds - int(3600 * hours) - int(minutes * 60))
        if hours:
            output = "{}h {}m".format(hours, minutes)
        elif minutes:
            output = "{}m {}s".format(minutes, seconds)
        else:
            output = "{}s".format(seconds)
        return output

    def print_progress_update(self):
        completed_entries = sum(self.process_entry_counts)
        complete_percentage = round(completed_entries / self.total_entries * 100)
        current_time = timeit.default_timer()
        self.elapsed_time = current_time - self.start_time
        self.predicted_time = round(self.total_entries / completed_entries * self.elapsed_time)

        text = "\r{}% complete; {}/{} entries. ".format(
                    complete_percentage, completed_entries, self.total_entries)
        text += " Time: {}; Predicted Remaining: {}; Active Processes: {}".format(\
                    self.format_to_shortest_time(self.elapsed_time),
                    self.format_to_shortest_time(self.predicted_time),
                    sum(self.active_processes)
                    )

        sys.stdout.write(text)
        sys.stdout.flush()

    def check_progress(self):
        change = self.update_entry_counts()
        if self.timer_counter >= SECONDS_PER_UPDATE and change:
            self.print_progress_update()
            self.timer_counter = 0
        else:
            self.timer_counter += 1

    def download_is_active(self):
        return any(self.active_processes)


# def download_all_files():
    
#     # FUNCTION VARIABLES
    
#     # Download Variables
#     entries_per_request = 100
#     entry_limit_per_file = 10000
#     file_base = "datafile_{}.json"
    
#     # Request Parameters
#     full_start = "2014-05-01 00:00"
#     full_end = datetime.datetime.now().strftime("%m/%d/%Y 23:59")
#     download_params = {
#             "start": full_start,
#             "end": full_end,
#             "limit": str(entries_per_request)
#             }
    
#     url_base = "https://archive.lco.global/frames?"
#     encoded_params = urllib.parse.urlencode(download_params)
#     url = url_base + encoded_params
    
#     # Initial Request to check success and get total entry number
#     r = requests.get(url=url)
    
#     # If Request fails, print reason
#     if r.status_code != 200:
#         print("Request Failed. Status Code: {}; Reason: {}".format(\
#               r.status_code, r.reason))
#         return
    
#     # Check length of download
#     request_data = r.json()
#     total_count = request_data['count']
#     print("{} entries to download. This will create {} files.".format(\
#           total_count, int(total_count/entry_limit_per_file)+1))
    
#     yn_continue = input("Continue with download? [Y/n] ")
#     if yn_continue not in ("", "y", "Y"):
#         print("Download aborted.")
#         return

#     # Variables for Download Loop
#     entry_counter = 0
#     file_count = 0
#     next_file_limit = entry_limit_per_file
#     temp_frame_list = []
    
#     # Main download loop
#     while True:
#         sys.stdout.write("\rDownloaded {} / {}  -  {}% Complete".format(\
#                          entry_counter, total_count,
#                          int(entry_counter/total_count*100)))
#         sys.stdout.flush()
        
#         if entry_counter >= next_file_limit:
#             # Reached the number of entries for this file.
#             # Save them to file and start compiling the next one.
#             file_path = pathjoin(dir_base, file_base.format(file_count))
#             with open(file_path, "w") as openfile:
#                 openfile.write(json.dumps(temp_frame_list))
#             temp_frame_list = []
#             next_file_limit += entry_limit_per_file
#             file_count += 1
            
#         results = request_data['results']
#         for frame in results:
#             temp_frame_list.append(frame)
            
#         if not request_data['next']:
#             break
        
#         next_url = request_data['next']
#         entry_counter += entries_per_request
        
#         r = requests.get(url=next_url)
#         if r.status_code != 200:
#             print("Request Failed. Status Code: {}; Reason: {}".format(\
#                   r.status_code, r.reason))
#             return
        
#         request_data = r.json()
        
#     # Exited Loop, save leftover entries.
#     file_path = pathjoin(dir_base, file_base.format(file_count))
#     with open(file_path, "w") as openfile:
#         openfile.write(json.dumps(temp_frame_list))
        
#     with open(completefile_path, "w") as cfile:
#         cfile.write(str(datetime.datetime.now()))
    
#     print("\nDownload of {} entries completed.".format(total_count))

def aggregate_downloaded_files():
    for filename in os.listdir(dir_base):
        pass
        
    
if __name__ == "__main__":
    # if os.path.isfile(pathjoin(dir_base, "_complete")):
    #    print("All files downloaded.")
    # else:
    #    download_all_files()
    # check_parameter_sizes()
    control_download_all_files()