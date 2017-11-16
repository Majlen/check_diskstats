#!/usr/bin/python3

import sys, re
from pathlib import Path
from shutil import copyfile, move
from glob import glob

tempDir = "/var/tmp/"
tempFile = "check_diskstat.py_"
validNums = ""
keys = ["reads", "reads_merged", "reads_sectors", "reads_ms",
        "writes", "writes_merged", "writes_sectors", "writes_ms",
        "io_in_progress", "io_ms", "io_ms_weighted"]

outKeys = ["tps", "read", "write", "avg_request_size", "avg_queue_size", "await"]

resolver = dict()

def main(argv):
    global validNums

    validNums = getValidMajorNums()

    tempName = tempDir + tempFile + "temp"

    #Read old stats
    statsTable = Path(tempName)
    if statsTable.exists():
        old = getStats(statsTable)
    else:
        #Copy stats
        copyfile('/proc/diskstats', tempName)
        print("Creating buffer")
        return 3

    #Copy new stats
    copyfile('/proc/diskstats', tempName)
    #Read new stats
    statsTable = Path(tempName)
    new = getStats(statsTable)

    initResolver()

    data = calculate(old, new)

    msg = "Summary of disks:\n"
    perfData = ""

    for (key, val) in data.items():
        msg += resolver[key] + ": Read %.0f kB/s; Write %.0f kB/s; %.0f IO/s\n" % (val['read'] / 1024, val['write'] / 1024, val['tps'])
        for k in outKeys:
            perfData += resolver[key] + ":" + k + "=%.2f " % val[k]
        perfData += ' '

    print(msg + '|' + perfData)

def calculate(oldArg, newArg):
    # Defined by kernel, does not depend on physical sector size
    sector = 512
    time = newArg['time'] - oldArg['time']

    out = dict()
    for (key, val) in newArg.items():
        if 'time' in key:
            continue

        new = newArg[key]
        old = oldArg[key]

        # fix 32bit overflow counter
        if new['reads_sectors'] < old['reads_sectors']:
            old['reads_sectors'] = old['reads_sectors'] - 4294967296
        if new['writes_sectors'] < old['writes_sectors']:
            old['writes_sectors'] = old['writes_sectors'] - 4294967296

        nr_ios = (new['reads'] - old['reads']) + (new['writes'] - old['writes'])

        data = list()

        read_bytes = ((new['reads_sectors'] - old['reads_sectors']) * sector) / time
        write_bytes = ((new['writes_sectors'] - old['writes_sectors']) * sector) / time
        tps = nr_ios / time

        aqusz = (new['io_ms_weighted'] - old['io_ms_weighted']) / time / 1000

        if nr_ios > 0:
            aw = ((new['reads_ms'] - old['reads_ms']) + (new['writes_ms'] - old['writes_ms'])) / nr_ios
            arqsz = ((new['reads_sectors'] - old['reads_sectors']) + (new['writes_sectors'] - old['writes_sectors'])) / nr_ios
        else:
            aw = 0
            arqsz = 0

        data.append(tps)
        data.append(read_bytes)
        data.append(write_bytes)
        data.append(arqsz)
        data.append(aqusz)
        data.append(aw)
        
        out[key] = dict(zip(outKeys, data))

    return out

def getValidMajorNums():
    devicesTable = Path('/proc/devices')
    headerFound = False
    majNums = list()

    with devicesTable.open() as read:
        while not headerFound:
            if "Block devices:" in read.readline():
                headerFound = True
        for line in read:
            arr = line.split()
            if "sd" in arr[1]:
                majNums.append(arr[0])

    return majNums

def getStats(statsTable):
    stats = dict()
    
    for line in statsTable.open():
        arr = line.split()
        # Skip partitions
        if arr[2][-1].isdigit():
            continue

        if arr[0] in validNums:
            vals = arr[3:]
            vals = list(map(int, vals))
            stats[arr[2]] = dict(zip(keys, vals))

    stats['time'] = statsTable.stat().st_mtime
    return stats

def initResolver():
    names = '/dev/disk/by-id/'
    types = ['ata', 'scsi']

    r = re.compile('.*-part[0-9]+$')

    for t in types:
        arr = list()
        arr.extend(glob(names + t + "*"))
        for src in arr:
            # Skip partitions
            if r.match(src):
                continue

            dest = Path(src).resolve().name
            srcID = str(Path(src).name).split('-', 1)[1]
            resolver[dest] = srcID

if __name__ == "__main__":
    main(sys.argv)
