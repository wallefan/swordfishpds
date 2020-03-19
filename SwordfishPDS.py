import csv
import http.client
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
        failed_downloads[filename_from_url(url)] = e
        return None
    return resp

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

            maybe_filename = item[-1]
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
                connection.request('HEAD', self.urltemplate.format(*item), headers={'User-Agent': 'SwordfishPDS-1.0'})
                with connection.getresponse() as resp:
                    if resp.code != 200:
                        # Single writes to sys.stdout are atomic.  Calls to print(), which make multiple writes to
                        # sys.stdout, are not.
                        sys.stdout.write(f'Error {resp.code} on {maybe_filename}')
                        self.failed_downloads.append(maybe_filename)
                        if resp.headers['Connection']=='Keep-Alive':
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
                connection.request('GET', self.urltemplate.format(*item), headers={
                    'User-Agent': 'SwordfishPDS-1.0',
                    'Range': f'bytes={my_length}-'
                })
            else:
                fout=open(output_path, 'wb')
                connection.request('GET', self.urltemplate.format(*item), headers={
                    'User-Agent': 'SwordfishPDS-1.0',
                })
            with fout, connection.getresponse() as resp:
                if resp.code == 416:  # 416 Range Not Satisfiable
                    # We've already got the whole file.
                    sys.stdout.write('%s is already up to date.\n'%filename)
                    if resp.headers.get('Connection') == 'Keep-Alive':
                        resp.read()  # work around bug in http.client.
                    continue
                elif resp.code != 200 and resp.code != 206:  # 200 OK, or 206 Partial Response for Range header
                    self.failed_downloads[filename] = '%d %s' % (resp.code, resp.reason)
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
            assert item is None, "Entry %s still in queue when join() was called!" % item
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
                    fout = open(filename, 'wb')
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
                resp = download(url, self.failed_downloads)
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
    mods_dir = os.path.join(outdir, '.minecraft', 'mods')
    os.makedirs(mods_dir, exist_ok=True)
    zip_downloader = ZipDownloader()
    other_stuff_downloader = ArbitraryURLDownloader()
    try:
        with open(os.path.join(outdir, 'SwordfishPDS-PackVersion.txt')) as f:
            version = f.read().strip()
        version, buildinfo = parse_version(version)
    except:
        version = (0, 0, 0)
        buildinfo = None
    with f:
        for type, *arg in csv.reader(f):
            if type == 'MOD':
                mod_downloader.start(THREADS)
                modid, filename = arg
                assert modid.isdigit() and len(modid) <= 7
                a = int(modid[:-3])
                b = int(modid[-3:])
                mod_downloader.put(a, b, filename, mods_dir)
            elif type == 'Zipfile':
                # Make each zip download its own thread for parallel extraction.
                url, output_dir, max_version = arg
                print('encountered zipfile directive', url, output_dir, max_version)
                max_version, max_buildinfo = parse_version(max_version)
                if version < max_version:
                    zip_downloader.start(ZIP_THREADS)
                    os.makedirs(output_dir, exist_ok=True)
                    zip_downloader.put(url, filename)
            elif type == 'Download':
                url, filename = arg
                other_stuff_downloader.start(THREADS)
                other_stuff_downloader.put(url, filename)
            elif type == 'Version':
                new_version, = arg
                version, buildinfo = parse_version(new_version)

    for _dl in (mod_downloader, zip_downloader, other_stuff_downloader):
        _dl.stop()

    print('=====================')
    any_ = False
    for _dl in (mod_downloader, zip_downloader, other_stuff_downloader):
        if _dl.failed_downloads:
            print('Some', _dl, 'failed to download:')
            for i in _dl.failed_downloads:
                print('-', i)
            any_ = True

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
        print('===============  S U C C E S S  ================')
        with open(os.path.join(output_dir, 'SwordfishPDS-PackVersion.txt'), 'w') as f:
            f.write('%d.%d.%d' % version)
            if buildinfo:
                f.write('+')
                f.write(buildinfo)


def parse_version(version):
    version, _, buildinfo = version.partition('+')
    assert version.count('.') == 2, 'invalid version number ' + version
    major, minor, patch = version.split('.')
    if major.lower() == 'x':
        major = 999
    else:
        major = int(major)
    if minor.lower() == 'x':
        minor = 999
    else:
        minor = int(minor)
    if patch.lower() == 'x':
        patch = 999
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
        print('===========================================')
        for i, pack in enumerate(available_packs):
            print(' %d. %s' % (i+1, pack))
        choice = ask_user(available_packs, 'Which pack do you want to download? ')
        f.write(available_packs[choice])
        f.write('\n')
        f.flush()
        # sockets are IO ref counted and don't actually close until all socket.makefile()s on them close,
        # so we can safely do this.
        return f, available_packs[choice]

def ask_user(options, prompt='Choose an option: '):
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
    connect_ip = '73.71.247.208'
    connect_port = 21617
    for arg in sys.argv[1:]:
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
            print('SwordfishPDS.py [{path to csv file|server_ip[:server_port]] [output_directory]')
            print('If no CSV file is provided, the script will connect to the specified server,')
            print('download a list of CSV files, and ask you to choose one.  If no server is')
            print('specified, the hardcoded default is 73.71.247.208 (the default server IP for')
            print('all SFE modpacks).  Port, if omitted, defaults to 21617.')
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
