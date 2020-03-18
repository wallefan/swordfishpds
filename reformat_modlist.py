import csv
import sys

def reformat(infilename, outfilename):
    with open(infilename, 'r', newline='') as infile:
        # Read in the archaic and terrible format into a list of lists
        mods = []
        for in_line in infile:
            in_line = in_line[4:] if "MODS" in in_line else in_line
            if "CONF" in in_line: 
                break
            mod_id = in_line[:7]
            mod_fname = in_line[7:].rstrip()
            if mod_id.isdigit():
                mods.append(["MOD", mod_id, mod_fname])
    # Now that we have a list, write it back out
    with open(outfilename, 'w', newline='') as outfile:
        csv.writer(outfile).writerows(mods)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Not enough arguments!\nUsage:reformat_modlist.py [INPUT] [OUTPUT]")
        sys.exit(-1)
    print("Reformatting modlist {0} into {1}.".format(sys.argv[1], sys.argv[2]))
    reformat(sys.argv[1], sys.argv[2])
