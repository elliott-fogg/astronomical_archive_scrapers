from spotipy.client import SpotifyException
import os
import sys
import time
import json
import shared_functions as sf
from requests.exceptions import ReadTimeout
from OpenSSL.SSL import WantReadError
from urllib3.exceptions import ReadTimeoutError, MaxRetryError
from requests.exceptions import ConnectionError


class CrawlerBase():
    def __init__(self, dirname, items_per_file=10000, count_threshold=100,
                 estimate_time=False):
        self.searched_items = {}        # Data for current searched-but-unsaved items
        self.saved_items = set()        # IDs for saved items
        self.unsearched_items = set()   # IDs for unsearched items
        # self.invalid_items = set()      # IDs for items that failed a search
        
        # Overwrite with a unique directory for each individual crawler.
        # All data folders should be in a universal \data folder.
        self.data_folder = "data"
        self.dirname = os.path.join(self.data_folder, dirname)
        os.makedirs(self.dirname, exist_ok=True)    # Ensure output folder exists
        self.saved_prefix = "saved"
        self.searched_filepath = os.path.join(self.dirname, "searched.json")
        self.unsearched_filepath = os.path.join(self.dirname, "unsearched.json")
        # self.invalid_filepath = os.path.join(self.dirname, "invalid.json")

        # Record the download speed / progress, for analysis
        self.analysis_data = []
        self.analysis_filepath = os.path.join(self.dirname, "analysis_data.json")
        
        # Derive certain statistics from the analysis file
        self.current_start_time = None
        self.past_runtime = 0
        self.starting_item_count = 0
        
        # Store additional input parameters
        self.items_per_file = items_per_file   # Num artists in each saved file
        self.count_threshold = count_threshold   # Update display every threshold
        self.estimate_time = estimate_time   # Whether to include an ETA
        # Crawling Artist IDs cannot have an ETA as we don't know how many there
        # will be.
        
        # Attempt to load the Saved, Searched, and Unsearched data
        self.load_saved_data()

        if len(self.unsearched_items) == len(self.searched_items) == \
            len(self.saved_items) == 0:
            # This is the first run of the crawler, run the initial setup
            self.initial_setup()

        self.show_start_printout()

        self.crawl()
    
    ### Save / Load Data #######################################################
    
    def load_saved_data(self):
        # Load saved items
        all_filenames = os.listdir(self.dirname)
        filenames = [f for f in all_filenames if self.saved_prefix in f]
        print("Loading saved files...")
        count = 0
        for filename in filenames:
            filepath = os.path.join(self.dirname, filename)
            with open(filepath, "r") as f:
                data = json.load(f)
            self.saved_items.update(list(data.keys()))
            count += 1
            if count % 10 == 0:
                self.reprint(f"{count} / {len(filenames)}")

        searched_data = self.load_with_backup(self.searched_filepath)
        if searched_data != None:
            self.searched_items = searched_data

        unsearched_data = self.load_with_backup(self.unsearched_filepath)
        if unsearched_data != None:
            self.unsearched_items = set(unsearched_data)

        # invalid_data = self.load_with_backup(self.invalid_filepath)
        # if invalid_data != None:
        #     self.invalid_items = set(invalid_data)

        # Load Analysis
        self.load_analysis()


    def save_analysis(self):
        current_runtime = time.time() - self.current_start_time + \
                                                            self.past_runtime
        current_item_count = len(self.saved_items) + len(self.searched_items)
        self.analysis_data.append( (
                                    current_runtime,
                                    current_item_count,
                                    len(self.unsearched_items)
                                       )
                                )
        analysis_to_save = {
            "current_item_count": current_item_count,
            "current_runtime": current_runtime,
            "analysis_data": self.analysis_data
        }

        with open(self.analysis_filepath, "w") as f:
            json.dump(analysis_to_save, f)


    def load_analysis(self):
        if os.path.isfile(self.analysis_filepath):
            analysis_to_load = json.load(open(self.analysis_filepath, "r"))
            self.starting_item_count = analysis_to_load["current_item_count"]
            self.past_runtime = analysis_to_load["current_runtime"]
            self.analysis_data = analysis_to_load["analysis_data"]


    def save_results_subset(self):
        subset = {}
        count = 0
        for key in self.searched_items:
            subset[key] = self.searched_items[key]
            count += 1
            if count >= self.items_per_file:
                break

        filepath = self.index_savefile_path()
        with open(filepath, "w") as f:
            json.dump(subset, f)

        items_saved = list(subset.keys())
        self.saved_items.update(items_saved)

        for item in items_saved:
            del self.searched_items[item]

        self.save_current_info()


    def save_with_backup(self, filepath, data):
        if os.path.isfile(filepath):
            # Delete previous backup file, if it exists
            backup_filepath = self.backup_name(filepath)
            if os.path.isfile(backup_filepath):
                os.remove(backup_filepath)
            # Make pre-existing file the backup
            os.rename(filepath, backup_filepath)

        # Save up-to-date copy of file
        with open(filepath, "w") as f:
            json.dump(data, f)


    def load_with_backup(self, filepath):
        # Attempt to load file
        if os.path.isfile(filepath):
            try:
                with open(filepath, "r") as f:
                    return json.load(f)

            except json.decoder.JSONDecodeError:
                # File is corrupted. Load backup instead.
                backup_filepath = self.backup_name(filepath)
                if os.path.isfile(backup_filepath):
                    with open(backup_filepath, "r") as f:
                        return json.load(f)


    def save_current_info(self):
        self.save_with_backup(self.searched_filepath, self.searched_items)
        self.save_with_backup(self.unsearched_filepath, list(self.unsearched_items))
        # self.save_with_backup(self.invalid_filepath, list(self.invalid_items))

        # Save Analysis
        self.save_analysis()


    def index_savefile_path(self):
        index = 0
        while True:
            filename = "{}_{}.json".format(self.saved_prefix, index)
            filepath = os.path.join(self.dirname, filename)
            if os.path.isfile(filepath):
                index += 1
            else:
                return filepath


    def backup_name(self, path):
        path_array = path.split(".")
        path_array.insert(-1, "_backup.")
        return "".join(path_array)


    def collate_results(self):
        print("\nCollating results...")
        all_results = {}
        for filename in os.listdir(self.dirname):
            if self.saved_prefix in filename:
                filepath = os.path.join(self.dirname, filename)
                data = json.load(open(filepath, "r"))
                all_results.update(data)

        output_name = f"{os.path.basename(os.path.normpath(self.dirname))}.json"
        output_filepath = os.path.join(self.data_folder, output_name)
        json.dump(all_results, open(output_filepath, "w"))
        print(f"Data collated into file: {output_filepath}")


    def log(self, message):
        logpath = os.path.join(self.dirname, "log.txt")
        with open(logpath, "a+") as l:
            l.write(f"{message}\n")

    ### Data Functions #########################################################
    # These functions are the ones to be replaced in each Crawler
    ############################################################################

    def initial_setup(self):
        for i in range(103):
            self.unsearched_items.add(i)


    def get_items_to_search(self):
        items_to_search = []
        count = 0
        for item in self.unsearched_items:
            items_to_search.append(item)
            count += 1
            if count >= 2:
                break
        return items_to_search


    def make_search_request(self, items_to_search):
        results = []
        for item in items_to_search:
            time.sleep(1)
            results.append("test")
        return results


    def process_search_results(self, items_to_search, results):
        for i in range(len(items_to_search)):
            self.searched_items[items_to_search[i]] = results[i]
        return items_to_search


    def clear_searched_items(self, items_searched):
        self.unsearched_items.difference_update(items_searched)


    def search_items(self, items_to_search):
        sleep_times = [0, 0.01, 0.1, 1, 5, 10]
        success = False

        for t in sleep_times:
            time.sleep(t)
            try:
                results = self.make_search_request(items_to_search)
                success = True
                break

            except (WantReadError, ReadTimeout,
                    ReadTimeoutError, ConnectionError) as e:
                log_message = f"Timeout ({t}): {e}"
                self.log(log_message)
                continue

        if not success:
            print("")
            print(f"\n{items_to_search}")
            print("Could not process request even after 10s wait.")
            raise

        else:
            return self.process_search_results(items_to_search, results)


    # def check_individual_items(self, items_to_reject):
    #     for item in items_to_reject:
    #         self.search_items([item])


    # def reject_items(self, items_to_reject):
    #     # Add items to invalid items
    #     # Remove items from unsearched items
    #     # Save
    #     pass


    ### Other Functions ########################################################

    def show_start_printout(self):
        print(f"Saving files in: {self.dirname}")
        print(f"Current Saved Items: {len(self.saved_items)}")
        print(f"Current Searched Items (not saved): {len(self.searched_items)}")
        print(f"Current Unsearched Items: {len(self.unsearched_items)}")


    def show_info_printout(self, endrun=False):
        metrics = self.calculate_metrics()

        if endrun:
            print("\n-Current run-")
            print("Runtime: {}, Items Searched: {}, Item Rate: {}/s".format(
                self.get_time_string(metrics["current_runtime"]),
                metrics["new_items"],
                round(metrics["item_rate"], 1)
            ))
            print("\n-Total-")

        if self.estimate_time:
            message = "Time Taken: {}, Progress: {} / {} ({:.2f}%), ETR: {}".format(
                self.get_time_string(metrics["total_runtime"]),
                metrics["total_searched"],
                metrics["total_items"],
                round(metrics["current_percent"]*100, 2),
                self.get_time_string(metrics["etr"], show_days=False)
        )

        else:
            message = "Time Taken: {}, Saved/Current/Unsearched: {} / {} / {} ".format(
                self.get_time_string(metrics["total_runtime"]),
                len(self.saved_items),
                len(self.searched_items),
                len(self.unsearched_items)
            )

        self.reprint(message, endrun)


    def get_time_string(self, seconds, show_days=True):
        ts = time.gmtime(seconds)
        time_string = ""
        if (ts.tm_yday > 1) and show_days:
            time_string += "{}d {:0>2d}h ".format(ts.tm_yday - 1, ts.tm_hour)
        elif (ts.tm_hour > 0) or (ts.tm_yday > 1):
            time_string += "{:0>2d}h ".format(ts.tm_hour + 24*(ts.tm_yday - 1))
        time_string += "{:0>2d}m {:0>2d}s".format(ts.tm_min, ts.tm_sec)
        return time_string


    def reprint(self, message, newline=False):
        text = f"\r{message}\033[K"
        if newline:
            text += "\n"
        sys.stdout.write(text)
        sys.stdout.flush()

    ### Calculations ###########################################################

    def calculate_metrics(self):
        total_searched = len(self.searched_items) + len(self.saved_items)
        new_items = total_searched - self.starting_item_count
        total_items = total_searched + len(self.unsearched_items)
        current_runtime = time.time() - self.current_start_time
        total_runtime = current_runtime + self.past_runtime
        item_rate = new_items / current_runtime
        current_percent = total_searched / total_items
        remaining_percent = 1 - current_percent
        percent_per_second = current_percent / total_runtime
        estimated_time_remaining = remaining_percent / percent_per_second

        return {
            "total_searched": total_searched,
            "new_items": new_items,
            "total_items": total_items,
            "current_runtime": current_runtime,
            "total_runtime": total_runtime,
            "item_rate": item_rate,
            "current_percent": current_percent,
            "etr": estimated_time_remaining
        }


    ### Main Crawl #############################################################

    def crawl(self):
        self.current_start_time = time.time()
        local_count = 0
        complete = False

        try:
            while True:

                # Exit loop if no more items to search
                if len(self.unsearched_items) == 0:
                    self.save_results_subset()
                    complete = True
                    break

                # If over file limit, save subset
                if len(self.searched_items) >= self.items_per_file:
                    self.save_results_subset()

                items_to_search = self.get_items_to_search()

                items_searched = self.search_items(items_to_search)

                self.clear_searched_items(items_searched)

                local_count += len(items_searched)

                if local_count >= self.count_threshold:
                    self.show_info_printout()
                    self.save_current_info()
                    local_count = 0

        except KeyboardInterrupt:
            self.reprint("Crawl Interrupted. Saving data...", True)
            self.save_current_info()

        finally:
            if complete:
                self.reprint("Crawl Complete!", True)
            self.show_info_printout(True)
            if complete:
                self.collate_results()


class SpotipyCrawlerBase(CrawlerBase):
    """
    A base object with centralised code for all the Spotify Crawlers to inherit.
    There are currently aiming to be 4 crawlers:
        * Crawl each artist ID for connected artists, to get all artist IDs
        * Crawl each artist ID for the artist information (followers, genres,...)
        * Crawl each artist ID for their top 10 tracks
        * Crawl each track ID for their audio features
    """

    def __init__(self, dirname, items_per_file=10000, count_threshold=100,
                 estimate_time=False):
        self.sp = sf.create_spotipy_accessor()
        super().__init__(dirname, items_per_file, count_threshold,
                         estimate_time)


if __name__ == "__main__":
    SpotipyCrawlerBase("DUMMY_FOLDER", items_per_file=10, count_threshold=5,
                       estimate_time=False)