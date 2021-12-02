# coding: utf-8
# encoding= utf-8

import re
from optparse import OptionParser

def split_a_line(line):
    fields = []
    list_s = line.split("|")
    list_s.pop(0)
    list_s.pop(-1)

    for f in list_s:
        fields.append(f.strip())
    return(fields)

# Pretty format wrap a long field to multple rows. This func reverts multiple rows to one row.
def combine_a_row(splitted_rows_list):
    row=[]

    for j in range(splitted_rows_list[0].__len__()):
        x = ''
        for k in splitted_rows_list.keys():
            x += splitted_rows_list[k][j]
        row.append(x)

    return(row)


def main():
    parser = OptionParser(usage="revert pretty formatted table to csv format\n\n%prog [options]")
    parser.add_option("-f", "--file", dest="file", help="file contains pretty formatted table.")
    parser.add_option("-d", "--debug",   action="store_true", dest="debug", default=False, help="enable debug log")

    (options, args) = parser.parse_args()

    if not options.file:
        parser.print_help()
        exit()

    if options.debug:
        print("reading "+options.file)

    text_file = open(options.file, "r")
    Lines = text_file.readlines()
    text_file.close()

######
    '''
    indicate current line type.
    row_marker: +---+--------
    row_data: |col1| col2 | col3 |
    '''
    bool_row_marker=False
    bool_row_data=False

    '''
    pretty format a long filed to multiple lines.
    '''
    wrap_rows={}

    for line in Lines:
        line = line.strip()

        if options.debug:
            print("DEBUG, "+line)

        if re.match(r'^\+\-', line):
            bool_row_data=False
            bool_row_marker=True
        elif re.match(r'^\|', line):
            bool_row_data=True
            bool_row_marker=False

        if bool_row_marker:
            #combine previous rows
            if wrap_rows.__len__()>0:
                row = combine_a_row(wrap_rows)
                str_row = ','.join(row)
                print(str_row)

            #reinit
            wrap_rows = {}
            wrap_row_idx=0

        if bool_row_data:
            wrap_rows[wrap_row_idx] = split_a_line(line=line)
            wrap_row_idx += 1

### MAIN ####
if __name__ == '__main__':
    main()