#!/usr/bin/env python3
import atexit
import csv
import http.client
import http.cookiejar
import os
import queue
import socket
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import zipfile

ZIP_THREADS = 5
THREADS = 3
SERVER_MODE = False  # set to True by __main__

cookiejar = http.cookiejar.MozillaCookieJar('cookies.txt')
try:
    cookiejar.load()
except (FileNotFoundError, http.cookiejar.LoadError):
    pass

atexit.register(cookiejar.save)

opener = urllib.request.build_opener()
opener.add_handler(urllib.request.HTTPCookieProcessor(http.cookiejar.MozillaCookieJar('cookies.txt')))
urllib.request.install_opener(opener)

# This global variable becomes true when we are done prompting the user about things,
# and other threads are now free to start writing to stdout.
allow_other_threads_to_print = False

#######################################################
#######          DOWNLOAD MACHINERY           #########
#######################################################


def extract_filename(resp, fallback=None):
    content_disposition = resp.headers.get('Content-Disposition')
    if content_disposition:
        for part in content_disposition.split(';'):
            if part.startswith('filename='):
                return part[9:].strip('"')
                break
    return fallback or filename_from_url(resp.geturl())


def filename_from_url(url):
    if '?' in url:
        return url
    path = urllib.parse.urlsplit(url).path
    return path[path.rfind('/') + 1:]


def get_content_length(resp, default=0):
    length = resp.getheader('Content-Length', default)
    try:
        return int(length)
    except ValueError:
        return 0


def copyfileobj(fin, fout, filename='', sz=0):
    buffer = bytearray(64 * 1024)
    bufsz = 64 * 1024
    t = time.perf_counter()
    total = 0
    while True:
        n = fin.readinto(buffer)
        if n == bufsz:
            fout.write(buffer)
        elif n == 0:
            return
        else:
            with memoryview(buffer)[:n] as view:
                fout.write(view)
        total += n
        if time.perf_counter() >= t + 1:
            # Calls to sys.stdout.write() are atomic.  Calls to print() are not.
            if sz:
                sys.stdout.write('Downloading %s (%.1f%% complete)\n' % (filename, (total * 100) / sz))
            else:
                sys.stdout.write('Downloading %s (%d bytes transferred)\n' % (filename, total))
            t = time.perf_counter()


def download(url, failed_downloads):
    try:
        resp = urllib.request.urlopen(url)
    except Exception as e:
        if isinstance(url, urllib.request.Request):
            url = url.get_full_url()
        failed_downloads[filename_from_url(url)] = e
        return None
    return resp


def _strip_dot_minecraft(filename):
    if SERVER_MODE:
        if filename.startswith('.minecraft'):
            filename = filename[10:]
            if filename[0] == '/':
                filename = filename[1:]
    return filename

class Downloader:
    def __init__(self, host, urlformat, tag=''):
        """


        :param host: The FQDN of the host to connect to. (Everything before the first slash in the url.)
        :param urlformat: Everything after (and including) the first slash in the URL.  Should contain str.format()
        paramters (i.e. {0}) that will be substituted with values passed to put().
        :param dir: Path to the local directory we should dump our files in.
        :param tag: Human readable string of what this downloader is for.  Returned by str(downloader).
        """
        self.host=host
        self.urltemplate=urlformat
        self.queue=queue.Queue()
        # can't use a stopping boolean because of a race condition -- what if one thread is already blocked on get()
        # when self.stopping becomes true?
        self.threads = []
        self.failed_downloads = {}
        self.tag = tag

    def __str__(self):
        return str(self.tag)

    def _worker(self):
        self.threads.append(threading.current_thread())
        connection = http.client.HTTPSConnection(self.host)
        while True:
            item = self.queue.get()
            if item is None:
                connection.close()
                return
            # item will be a tuple that gets formatted into our template, except for the last element,
            # which is the output directory.
            # we assume that the second last element in this tuple is some sort of human readable filename,
            # or at least one we can fall back on if the server doesn't tell us what the actual filename is.
            output_dir = item[-1]
            item = item[:-1]

            urlpath = self.urltemplate.format(*item).replace(' ', '+')
            maybe_filename = urllib.parse.unquote(item[-1])
            if maybe_filename.endswith('.jar'):
                # then it is definitely a filename
                filename = maybe_filename
                # since we haven't asked the server how long the file is yet, we don't know.
                # we could see if the file exists locally and check that length, but the point of having this
                # length field is resuming interrupted downloads.
                # length = None means we have not checked.  length = -1 means we have checked and the server didn't
                # answer us.
                length = None
            else:
                # we can't be absolutely certain that it's a filename.  Best to double check.
                connection.request('HEAD', urlpath, headers={'User-Agent': 'SwordfishPDS-1.0'})
                with connection.getresponse() as resp:
                    if resp.code != 200:
                        # Single writes to sys.stdout are atomic.  Calls to print(), which make multiple writes to
                        # sys.stdout, are not.
                        sys.stdout.write(f'Error {resp.code} on {maybe_filename}')
                        self.failed_downloads.append(maybe_filename)
                        if resp.headers['Connection'] == 'keep-alive':
                            resp.read()  # known bug in http library.
                        continue
                    filename = extract_filename(resp) or maybe_filename
            # Now we know for certain what the filename is.
            # Why do we need to know what the local filename is before we make the request? To resume downloads,
            # of course!
            output_path = os.path.join(output_dir, filename)
            if os.path.exists(output_path):
                fout = open(output_path, 'ab')
                my_length=fout.tell()
                connection.request('GET', urlpath, headers={
                    'User-Agent': 'SwordfishPDS-1.0',
                    'Range': f'bytes={my_length}-'
                })
            else:
                fout=open(output_path, 'wb')
                connection.request('GET', urlpath, headers={'User-Agent': 'SwordfishPDS-1.0'})
            with fout, connection.getresponse() as resp:
                if resp.code == 416:  # 416 Range Not Satisfiable
                    # We've already got the whole file.
                    sys.stdout.write('%s is already up to date.\n'%filename)
                    if resp.headers.get('Connection') == 'keep-alive':
                        resp.read()  # work around bug in http.client.
                    continue
                elif resp.code != 200 and resp.code != 206:  # 200 OK, or 206 Partial Response for Range header
                    self.failed_downloads[filename] = '%d %s' % (resp.code, resp.reason)
                    print(resp.headers['Connection'])
                    if resp.headers['Connection'] == 'keep-alive':
                        resp.read()
                    continue
                copyfileobj(resp, fout, filename, get_content_length(resp))

    def start(self, nthreads):
        if self.threads:
            # No-op if we're already running.
            return
        for _ in range(nthreads):
            threading.Thread(target=self._worker).start()

    def stop(self):
        if not self.threads:
            return
        for _ in self.threads:
            self.queue.put(None)
        for thread in self.threads:
            thread.join()
        # Clear out the queue.
        while not self.queue.empty():
            item = self.queue.get_nowait()
            assert item is None, "Entry %s still in queue when join() was called!" % (item,)
        self.threads.clear()

    def put(self, *task):
        assert self.threads, "attempt to put a task in the queue while the thread pool was halted"
        self.queue.put(task)

class ArbitraryURLDownloader(Downloader):
    def __init__(self):
        super().__init__(None, None, 'files')
    def _worker(self):
        self.threads.append(threading.current_thread())
        while True:
            item = self.queue.get()
            if item is None:
                return
            url, dest = item
            if os.path.isfile(dest):
                # the path we have been passed is a file path, not a directory path,
                # and it points to a file that already exists on disk.
                # Resume download if possible.
                fout = open(dest, 'ab')
                filename = dest
                req = urllib.request.Request(url, headers={'User-Agent': 'SwordfishPDS-1.0',
                                                           'Range': 'bytes=%d-' % fout.tell()})
            else:
                # Don't trust that the last part of the URL is the filename.  It almost never is.
                fout = None
                filename = None
                req = urllib.request.Request(url, headers={'User-Agent': 'SwordfishPDS-1.0'})
            resp = download(req, self.failed_downloads)
            if resp is None:
                continue
            with resp:
                if fout is None:
                    filename = extract_filename(resp)
                    os.makedirs(dest, exist_ok =True)
                    fout = open(os.path.join(dest, filename), 'wb')
                with fout:
                    copyfileobj(resp, fout, filename, get_content_length(resp))


class ZipDownloader(Downloader):
    def __init__(self):
        super().__init__(None, None, 'ZIP files')

    def _worker(self):
        self.threads.append(threading.current_thread())
        while True:
            item = self.queue.get()
            if item is None:
                return
            url, dest = item
            with tempfile.TemporaryFile() as f:
                resp = download(urllib.request.Request(url, headers={'User-Agent': 'SwordfishPDS-1.0'}),
                                self.failed_downloads)
                if not resp:
                    continue
                copyfileobj(resp, f, extract_filename(resp), get_content_length(resp))
                try:
                    with zipfile.ZipFile(f) as zf:
                        zf.extractall(dest)
                except Exception as e:
                    self.failed_downloads[extract_filename(resp)] = e



# Necessary because Python, unlike Java, does not allow manual synchronization of I/O descriptors.
# Single calls to write() are atomic but print() makes multiple calls!
def print_thread(q):
    while True:
        item=q.get()
        if item is None: return
        print(item)

printq=queue.Queue()
threading.Thread(target=print_thread, args=(printq,), daemon=True).start()


##################################################
############ FILE FORMAT #########################
##################################################

def run(f, outdir, created_modpack=True):
    mod_downloader = Downloader('media.forgecdn.net', '/files/{0}/{1}/{2}', 'mods')
    mods_dir = os.path.join(outdir, 'mods') if SERVER_MODE else os.path.join(outdir, '.minecraft', 'mods')
    all_mods = []
    dud_mods = []
    os.makedirs(mods_dir, exist_ok=True)
    zip_downloader = ZipDownloader()
    other_stuff_downloader = ArbitraryURLDownloader()
    try:
        with open(os.path.join(outdir, 'SwordfishPDS-PackVersion.txt')) as vfile:
            version = vfile.read().strip()
        print('read version', version, 'from file')
        version, buildinfo = parse_version(version)
    except Exception as e:
        version = (0, 0, 0)
        buildinfo = None
        print('Could not load version from file!', e)
    print(version, buildinfo)
    with f:
        # ugly (or beautiful depending on how you look at it) python 3 hack for comment characters
        for type, *arg in csv.reader(filter(lambda line: not line.startswith('#'), f)):
            if type == 'MOD':
                mod_downloader.start(THREADS)
                modid, filename = arg
                all_mods.append(filename)
                # if the mod exists, but is disabled, don't download it again
                # as that both wastes time and confuses MultiMC.
                mod_path = os.path.join(mods_dir, filename)
                if os.path.exists(mod_path + '.disabled'):
                    os.rename(mod_path + '.disabled', mod_path)
                if not modid.strip():
                    # dud mod, will be downloaded by other means.  just add it to the mod list and move on
                    print('encountered dud mod', filename)
                    dud_mods.append(filename)
                    continue
                assert modid.isdigit() and len(modid) <= 7
                a = int(modid[:-3])
                b = int(modid[-3:])
                if a == b == 0:
                    continue  # ditto
                mod_downloader.put(a, b, filename, mods_dir)
            elif type == 'Zipfile':
                # Make each zip download its own thread for parallel extraction.
                url, dest_dir, max_version = arg
                max_version, max_buildinfo = parse_version(max_version)
                dest_dir = _strip_dot_minecraft(dest_dir)
                # If one or more downloads have already failed, fail fast and don't bother downloading the zip file,
                # since we're not going to update the version number anyway.
                if version < max_version and not zip_downloader.failed_downloads \
                        and not mod_downloader.failed_downloads and not other_stuff_downloader.failed_downloads:
                    zip_downloader.start(ZIP_THREADS)
                    dest_dir = os.path.join(outdir, dest_dir.replace('/', os.path.sep))
                    os.makedirs(dest_dir, exist_ok=True)
                    zip_downloader.put(url, dest_dir)
                if version > max_version:
                    sys.stdout.write('Skipping downloading %s because we are already up to date.\n'%url)
            elif type == 'Download':
                url, filename = arg
                other_stuff_downloader.start(THREADS)
                filename = _strip_dot_minecraft(filename)
                if os.path.exists(filename + '.disabled'):
                    os.rename(filename + '.disabled', filename)
                    # if the download got far enough to be disabled, assume it's good.
                    continue
                other_stuff_downloader.put(url, os.path.join(outdir, filename.replace('/', os.path.sep)))
            elif type == 'Version':
                new_version, = arg
                version, buildinfo = parse_version(new_version)
            elif type == 'Nuke':
                filename, = arg
                filename = os.path.join(outdir, _strip_dot_minecraft(filename))
                if os.path.exists(filename):
                    os.rename(filename, filename + '.disabled')

    surplus_mods = []
    installed_mods = os.listdir(mods_dir)
    for mod in installed_mods:
        if mod.endswith('.jar') and mod not in all_mods:
            surplus_mods.append(mod)
        elif mod in dud_mods:
            dud_mods.remove(mod)

    for _dl in (mod_downloader, zip_downloader, other_stuff_downloader):
        _dl.stop()

    any_ = False
    for _dl in (mod_downloader, zip_downloader, other_stuff_downloader):
        if _dl.failed_downloads:
            print('Some', _dl, 'failed to download:')
            for file, reason in _dl.failed_downloads.items():
                print(' - %s: %s'%(file, reason))
            any_ = True

    if dud_mods:
        any_ = True
        print('Some mods were listed as required but were not installed:')
        for mod in dud_mods:
            print(' -', mod)

    print('================================================')
    if any_:
        print('Please go yell at @Snek or @some dude 2000 miles away in Discord because')
        print('this is probably their fault.')
        print("========= D O W N L O A D   F A I L E D ========")
    else:
        if created_modpack:
            print('All done!  Modpack successfully installed.')
            print('You may need to restart MultiMC before the modpack appears.')
        else:
            print('All done!  Modpack successfully updated.')

        with open(os.path.join(outdir, 'SwordfishPDS-PackVersion.txt'), 'w') as vfile:
            vfile.write('%d.%d.%d' % version)
            if buildinfo:
                vfile.write('+')
                vfile.write(buildinfo)

        if surplus_mods:
            print('These mods are installed in your client but are not in the pack description:')
            for mod in surplus_mods:
                print('-', mod)
            choice = ask_user(["Leave them in (they won't get activated when you connect to the server)",
                               'Disable them (can be re-enabled in the Loader Mods tab in MultiMC)',
                               'Delete them'], 'What would you like to do?')
            if choice == 1:
                for mod in surplus_mods:
                    path_to_mod = os.path.join(mods_dir, mod)
                    try:
                        os.rename(path_to_mod, path_to_mod + '.disabled')
                    except FileExistsError:
                        os.unlink(path_to_mod)
            elif choice == 2:
                for mod in surplus_mods:
                    os.unlink(os.path.join(mods_dir, mod))

        print('===============  S U C C E S S  ================')
        print('Go launch your game.')


def parse_version(version):
    version, _, buildinfo = version.partition('+')
    assert version.count('.') == 2, 'invalid version number ' + version
    major, minor, patch = version.split('.')
    if major.lower() == 'x':
        major = -1
    else:
        major = int(major)
    if minor.lower() == 'x':
        minor = -1
    else:
        minor = int(minor)
    if patch.lower() == 'x':
        patch = -1
    else:
        patch = int(patch)
    return ((major, minor, patch), buildinfo)


def prompt_yn(prompt):
    while True:
        resp = input(prompt).strip().lower()[0]
        if resp == 'y':
            return True
        elif resp == 'n':
            return False


def locate_multimc_dir():
    import platform
    plat=platform.system()

    if plat=='Windows':
        candidates = [
            os.environ['USERPROFILE']+r'\MultiMC',
            os.environ['appdata']+r'\MultiMC',
            os.environ['USERPROFILE']+r'\Desktop\MultiMC',
        ]
    elif plat=='Linux':
        candidates = [
            os.path.expanduser('~/.local/share/multimc'),
            os.path.expanduser('~/MultiMC'),
            os.path.expanduser('~/multimc')
        ]
    else:
        # on MacOS, I have no idea
        candidates = []
    for candidate in candidates:
        if os.path.exists(candidate):
            print('Found MultiMC directory at:', candidate)
            if prompt_yn('Is this correct?  (If unsure, answer yes).\nType Y or N:'):
                return candidate
            else:
                # If the user answered "no", their install is probably in a nonstandard location.
                # Don't bother checking the other standard ones.
                break
    else:
        # If the user did not explicitly answer no, print this message.
        print("I couldn't automatically find your MultiMC folder.")
    print('Please point me in the right direction.  ')
    if platform.system() == 'Windows':
        print('1. Open MultiMC')
        print("2. Click on any pack you have installed, doesn't matter which")
        print('3. Click "Open Instance Folder" on the right, near the middle')
        print('4. Click in the path bar (just above the list of folders), Ctrl+C to copy the path')
        print('5. Come back to this window, right click to paste, then press Enter.')
        while True:
            path = input(': ').strip()
            if not path[1]==':' or path.count(':') > 1:
                # the user accidentally pasted the path twice, and/or put something before it.
                first_colon = path.find(':')
                second_colon = path.find(':', first_colon+1)
                path = path[first_colon-1:second_colon-1]
                path = path[:-2]
            if os.path.exists(path):
                head, tail = os.path.split(path)
                if head.endswith('instances'):
                    head2, tail2 = os.path.split(head)
                    return head2
                elif tail == 'instances':
                    # the user is smart and has already trimmed the path down to the instance folder.
                    return head
            print("Couldn't quite catch that.  Please try again.")
    else:
        # assume users of other systems have at least half a brain.
        while True:
            path = input('Please enter or paste the path to your MultiMC install directory: ')
            if not os.path.exists(path):
                print("That path doesn't exist, please try again")
                continue
            head, tail = os.path.split(path)
            if tail == 'instances':
                return head
            return path

def createMinecraftFolder(multimc_dir, instanceName, icon='default'):
    minecraft_dir = os.path.join(multimc_dir, 'instances', instanceName, '.minecraft')
    os.makedirs(os.path.join(minecraft_dir, 'mods'), exist_ok=True)
    os.makedirs(os.path.join(minecraft_dir, 'config'), exist_ok=True)
    instance_dir = os.path.join(multimc_dir, 'instances', instanceName)
    # Yes I am aware that this method of embedding files is horrendously ugly, but it's better than any of the
    # alternatives I could think of.  And the files have to get there somehow.
    if not os.path.exists(os.path.join(instance_dir, 'instance.cfg')) or icon != 'default':
        with open(os.path.join(instance_dir, 'instance.cfg'), 'w') as f:
            f.write(f"""InstanceType=OneSix
OverrideCommands=false
OverrideConsole=false
OverrideJavaArgs=false
OverrideJavaLocation=false
OverrideMemory=false
OverrideWindow=false
iconKey={icon}
name={instanceName}
notes=
""")
        with open(os.path.join(instance_dir, 'mmc-pack.json'), 'w') as f:
            f.write("""{
    "components": [
        {
            "cachedName": "LWJGL 2",
            "cachedVersion": "2.9.4-nightly-20150209",
            "cachedVolatile": true,
            "dependencyOnly": true,
            "uid": "org.lwjgl",
            "version": "2.9.4-nightly-20150209"
        },
        {
            "cachedName": "Minecraft",
            "cachedRequires": [
                {
                    "suggests": "2.9.4-nightly-20150209",
                    "uid": "org.lwjgl"
                }
            ],
            "cachedVersion": "1.12.2",
            "important": true,
            "uid": "net.minecraft",
            "version": "1.12.2"
        },
        {
            "cachedName": "Forge",
            "cachedRequires": [
                {
                    "equals": "1.12.2",
                    "uid": "net.minecraft"
                }
            ],
            "cachedVersion": "14.23.5.2847",
            "uid": "net.minecraftforge",
            "version": "14.23.5.2847"
        }
    ],
    "formatVersion": 1
}
""")
    # For the archaeologist digging through this code and wondering what this snippet is for:
    # Originally I was going to automatically detect an instance folder that had already been manually created.
    # I thought better of that idea halfway through writing it.

    # import difflib
    # possible_existing_instances = difflib.get_close_matches(instanceName, instances)
    # if possible_existing_instances:
    #     print('Found some existing packs in your MultiMC folder with a similar name.')
    #     print("If you already created an empty pack in MultiMC before running this script,")
    #     print("please choose one of the options below.  If you didn't, select 0 to create a")
    #     print('new instance.')

    return instance_dir


def connect(server):
    print('Connecting to', server[0], '...')
    with socket.create_connection(server) as s:
        s.setblocking(True)
        available_packs=[]
        f = s.makefile('rw')
        print('Retrieving modpack list...')
        for line in f:
            line=line.strip()
            if not line:
                break
            available_packs.append(line)
        if not available_packs:
            print('There are no packs available for download right now.  Please try again later.')
            input('Press Enter to quit.')
            exit()
        choice = ask_user(available_packs, 'Which pack do you want to download? ')
        f.write(available_packs[choice])
        f.write('\n')
        f.flush()
        # sockets are IO ref counted and don't actually close until all socket.makefile()s on them close,
        # so we can safely do this.
        return f, available_packs[choice]

def ask_user(options, prompt='Choose an option: '):
    print('===========================================')
    for i, option in enumerate(options):
        print(' %d. %s' % (i + 1, option))
    while True:
        choice = input(prompt)
        try:
            choice = int(choice)
        except ValueError:
            print('Please enter a number')
            continue
        if choice < 1:
            print("Yes, you're very smart.")
        elif choice > len(options):
            print('Please enter a number from 1 to', len(options))
        else:
            return choice - 1

if __name__=='__main__':
    output_dir = None
    file = None
    connect_ip = 'redbaron.local'
    connect_port = 21617
    for arg in sys.argv[1:]:
        if arg == '--server-mode':
            SERVER_MODE = True
        if os.path.isdir(arg):
            output_dir = arg
        elif os.path.isfile(arg):
            file = arg
        elif all(ch in '1234567890.:' for ch in arg):
            if ':' in arg:
                connect_ip, connect_port = arg.split(':')
                connect_port = int(connect_port)
            else:
                connect_ip = arg
        else:
            print('Usage:')
            print('SwordfishPDS.py [--server-mode] [{path to csv file|server_ip[:server_port]] [output_directory]')
            print('If no CSV file is provided, the script will connect to the specified server,')
            print('download a list of CSV files, and ask you to choose one.  If no server is')
            print('specified, the hardcoded default is 73.71.247.208 (the default server IP for')
            print('all SFE modpacks).  Port, if omitted, defaults to 21617.')
            print("--server-mode indicates that we're installing mods on the server rather than a client.")
            print('If specified, files will be placed directly in the destination directory rather than')
            print('destdir/.minecraft.')
            exit()
    # locate_multimc_dir() sometimes requires user intervention, so for the sake of seamlessness, skip it
    # if an output dir is specified.  Also do it before potentially connecting to the server.
    if output_dir is None:
        multimc_dir = locate_multimc_dir()

    pack_name = None
    if file is not None:
        f = open(file)
        pack_name = os.path.splitext(os.path.basename(file))[0]
    else:
        f, pack_name = connect((connect_ip, connect_port))
    if output_dir is None:
        output_dir = createMinecraftFolder(multimc_dir, pack_name)
    run(f, output_dir)
