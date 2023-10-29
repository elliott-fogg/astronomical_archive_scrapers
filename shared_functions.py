import os
import pathlib
import json
import pickle

##### Kill File Functions ##########################################################################
KILL_FILE_PATH = ".kill_file"

def reset_kill_file():
    with open(KILL_FILE_PATH, "w") as f:
        f.write("0")

def trigger_kill_file():
    with open(KILL_FILE_PATH, "w") as f:
        f.write("1")

def get_kill_status():
    if not os.path.isfile(KILL_FILE_PATH):
        return True

    with open(KILL_FILE_PATH) as f:
        data = f.read()

    if data == 0 or data == None:
        return False
    else:
        return True

##### Spotipy API ####################################################################################

def get_private_details():
    return json.load(open("spotify_credentials.private", "r"))

def create_spotipy_accessor():
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials

    creds = get_private_details()
    client_credentials_manager = SpotifyClientCredentials(client_id=creds['client_id'], client_secret=creds['client_secret'])
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    return sp


default_scope = 'user-library-read, playlist-read-collaborative, playlist-read-private, user-top-read, user-follow-read'

def get_user_permissions(scope=default_scope):
    import spotipy
    creds = get_private_details()
    client_id, client_secret, username = creds['client_id'], creds['client_secret'], creds['username']
    redirect_uri = "https://example.com/callback/"
    
    token = spotipy.util.prompt_for_user_token(
        username,
        client_id=client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri,
        scope=default_scope
    )
    
    if token:
        sp = spotipy.Spotify(auth=token)
        return sp
    else:
        return None
    

##### Other ##########################################################################################

def fwrite(filepath, text):
    with open(filepath, "w") as f:
        f.write(text)

def bwrite(filepath, text):
    with open(filepath, "wb") as fb:
        fb.write(text.encode('utf-8'))

def read_file(filepath):
    if os.path.isfile(filepath):
        with open(filepath, "r") as f:
            return f.read()
    else:
        return None

def pickle_dump(filepath, content):
    pickle.dump(content, open(filepath, "wb"))

def pickle_load(filepath):
    pickle.load(open(filepath, "rb"))

def yesno(text, yes_default=True):
    yn = input(text)
    accepted_answers = ['Y','y']
    if yes_default:
        accepted_answers += ['']
    if yn in accepted_answers:
        return True
    else:
        return False

def check_folder(folder_path):
    pathlib.Path(folder_path).mkdir(parents=True, exist_ok=True)

def index_name(filepath, index_always=False):
    # If the file does not exist, and index_always is False, return the filepath immediately
    if not (os.path.exists(filepath) or index_always) :
        return filepath

    dir_path, file_base = os.path.split(filepath)
    if "." in file_base:
        file_name, file_suffix = file_base.rpartition(".")
        file_suffix = "." + file_suffix
    else:
        file_name = file_base
        file_suffix = ""

    counter = 1
    while True:
        full_name = "".join([file_name, "_", str(counter), file_suffix])
        full_path = os.path.join(dir_path, full_name)
        if os.path.exists(full_path):
            break
        else:
            counter += 1

    return full_path

def log_error(folder, chart_type, country, date, reason, error_text="0"):
    error_file_name = "{}_{}_{}_{}.txt".format(reason, chart_type, country, date)
    error_file_path = path.os.join(folder, error_file_name)
    bwrite(error_file_path, error_text)


def something():
    pass
