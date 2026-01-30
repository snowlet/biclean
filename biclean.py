import csv
from tqdm import tqdm
import argparse
import re
import shutil


class CommaTqdm(tqdm):
    """
    A custom tqdm class that formats numbers with commas and handles rate display.
    Refactored to be a global class to avoid code duplication.
    """
    def __init__(self, *args, **kwargs):
        # Set default bar_format if not provided
        if 'bar_format' not in kwargs:
            kwargs['bar_format'] = "{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt} lines/s]"
        
        # Set default miniters and mininterval to force updates
        if 'miniters' not in kwargs:
            kwargs['miniters'] = 1
        if 'mininterval' not in kwargs:
            kwargs['mininterval'] = 0.1
            
        super().__init__(*args, **kwargs)

    @property
    def format_dict(self):
        # Get the standard dictionary
        d = super().format_dict
        
        # 1. Format the counter and total with commas
        d['n_fmt'] = f'{d["n"]:,}'
        d['total_fmt'] = f'{d["total"]:,}'
        
        # 2. Handle the rate (lines/s)
        # We check 'rate' specifically. If it is None, tqdm hasn't calculated it yet.
        rate = d.get('rate')
        if rate is not None:
            d['rate_fmt'] = f'{rate:,.2f}'
        else:
            # If rate is None, we can try to calculate it manually based on elapsed time
            # or default to '?' if elapsed is 0.
            elapsed = d.get('elapsed', 0)
            n = d.get('n', 0)
            if elapsed > 0:
                calc_rate = n / elapsed
                d['rate_fmt'] = f'{calc_rate:,.2f}'
            else:
                d['rate_fmt'] = '?'
        
        return d


def merge_bilingual_files(st_file, tt_file, merged_file):
    # Counting lines...
    with open(st_file, 'r', encoding='utf-8') as f:
        total_rows = sum(1 for _ in f)

    with open(st_file, 'r', encoding='utf-8') as st, \
         open(tt_file, 'r', encoding='utf-8') as tt, \
         open(merged_file, 'w', newline='', encoding='utf-8') as merged:
    
        merged_writer = csv.writer(merged)
        merged_writer.writerow(['index', 'st', 'tt'])

        with CommaTqdm(total=total_rows, desc="Merging ST & TT into CSV") as pbar:
            for index, (st_line, tt_line) in enumerate(zip(st, tt), start=1):
                st_content = st_line.rstrip('\n')
                tt_content = tt_line.rstrip('\n')
                merged_writer.writerow([index, st_content, tt_content])
                pbar.update(1)

    return total_rows


def convert_zh_tw_to_zh_cn(input_file, output_file, dirty_file=None):
    from opencc import OpenCC
    converter = OpenCC('t2s')  # Traditional Chinese to Simplified Chinese
    
    with open(input_file, 'r', encoding='utf-8') as f:
        total_rows = sum(1 for _ in f) - 1  # Exclude header line

    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile, \
         open(dirty_file, 'a', encoding='utf-8', newline='') if dirty_file else open('/dev/null', 'w') as dirty_outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        dirty_writer = csv.DictWriter(dirty_outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        dirty_writer.writeheader()
        tc_number = 0

        with CommaTqdm(total=total_rows, desc="Converting zh-TW to zh-CN") as pbar:
            for row in reader:
                tt_content = converter.convert(row["tt"])
                if row["tt"] != tt_content:
                    dirty_writer.writerow({"index": row["index"], "st": row["st"], "tt": row["tt"]})
                    tc_number += 1
                # After conversion, write to clean file anyway.
                writer.writerow({"index": row["index"], "st": row["st"], "tt": tt_content})
                pbar.update(1)

    return tc_number


def remove_st_in_tt(input_file, output_file, dirty_file=None):
    """
    Removes ST (from the second column) that exists at the end of the third column (TT).
    These rows imply bilingual content and only the ST are cleaned from the TT.
    The cleaned content is saved to output_file, and the bilingual contents are saved to dirty_file.
    Returns the number of bilingual lines.
    """
    # Counting lines...
    with open(input_file, 'r', encoding='utf-8') as f:
        total_rows = sum(1 for _ in f) - 1  # Exclude header line

    bilingual_lines = 0
    try:
        with open(input_file, "r", encoding="utf-8") as infile, \
             open(output_file, "w", encoding="utf-8", newline='') as outfile, \
             open(dirty_file, "w", encoding="utf-8", newline='') if dirty_file else open('/dev/null', 'w') as dirty_outfile:
            reader = csv.DictReader(infile)
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            dirty_writer = csv.DictWriter(dirty_outfile, fieldnames=reader.fieldnames)
            writer.writeheader()

            with CommaTqdm(total=total_rows, desc="Removing ST from TT") as pbar:
                for row in reader:
                    st = row["st"].strip()
                    tt = row["tt"].strip()
                    if tt.endswith(st):
                        bilingual_lines += 1
                        dirty_writer.writerow(row)
                        row["tt"] = tt[:-len(st)].strip()
                    elif tt.startswith(st):
                        bilingual_lines += 1
                        dirty_writer.writerow(row)
                        row["tt"] = tt[len(st):].strip()
                    writer.writerow(row)

                    pbar.update(1)

        return bilingual_lines
    except FileNotFoundError:
        print(f"Error: {input_file} not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


def remove_timestamps(input_file, output_file, dirty_file=None):
    """
    Removes .ass tags from the ST/TT column in the input file (.csv), and saves the cleaned content to the output file (.csv). Uncleaned contents will be saved to dirty_file (.csv).
    Time tags are quoted by square brackets, such as [00:01:23.45] or [00:01:23,45].
    Returns the number of lines with time tags removed.
    Columns of the .csv files: Index, ST, TT.
    """
    timestamp_patterns = []
    # 546 00:31:48,490 -- 00:31:49,865
    # 546 00:31:48,490 --> 00:31:49,865
    # 546 00:31:48.490 -- 00:31:49.865
    # 546 00:31:48.490 --> 00:31:49.865
    timestamp_patterns.append(
        re.compile(
            r'\b\d+\s+'                     # 546
            r'\d{2}:\d{2}:\d{2}[,\.]\d{3}'  # 00:31:48,490
            r'\s+--\s+'                     # -- (allow spaces around)
            r'\d{2}:\d{2}:\d{2}[,\.]\d{3}'  # 00:31:49,865
        )
    )
    # 01:19:31,500...  01:19:32,832
    # 01:19:31.500...  01:19:32.832 
    timestamp_patterns.append(
        re.compile(
            r'\d{2}:\d{2}:\d{2}[,\.]\d{3}'  # 00:31:48,490
            r'\.+\s+'                       # separator: ... (allow spaces around)
            r'\d{2}:\d{2}:\d{2}[,\.]\d{3}'  # 00:31:48,490
        )
    )
    # Comment: 0,0:00:01.00...              # "Comment" can be any word
    timestamp_patterns.append(
        re.compile(
            r'\b[A-Za-z]+:\s*\d+,\d+:\d{2}:\d{2}[.,]\d{2,3}\.\.\.'
        )
    )
    # 546 00:31:48,490
    # 546 00:31:48.490
    timestamp_patterns.append(
        re.compile(
            r'\b\d+\s+'                     # 546
            r'\d{2}:\d{2}:\d{2}[,\.]\d{3}'  # 00:31:48,490
        )
    )
    # [00:01:23.45] or [00:01:23,45]
    timestamp_patterns.append(
        re.compile(
            r'\[(\d{2}:\d{2}:\d{2}[.,]\d{2})\]'
        )
    )
    
    # Counting lines...
    with open(input_file, 'r', encoding='utf-8') as f:
        total_rows = sum(1 for _ in f) - 1  # Exclude header line

    timestamp_lines = 0
    try:
        with open(input_file, "r", encoding="utf-8") as infile, \
             open(output_file, "w", encoding="utf-8", newline='') as outfile, \
             open(dirty_file, "a", encoding="utf-8", newline='') if dirty_file else open('/dev/null', 'w') as dirty_outfile:
            reader = csv.DictReader(infile)
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            dirty_writer = csv.DictWriter(dirty_outfile, fieldnames=reader.fieldnames)
            writer.writeheader()

            with CommaTqdm(total=total_rows, desc="Cleaning timestamps from ST & TT") as pbar:
                for row in reader:
                    st = row["st"].strip()
                    tt = row["tt"].strip()
                    for timestamp_pattern in timestamp_patterns:
                        st_cleaned = timestamp_pattern.sub("", st)
                        tt_cleaned = timestamp_pattern.sub("", tt)
                        st_cleaned = re.sub(r'\s{2,}', ' ', st_cleaned).strip()
                        tt_cleaned = re.sub(r'\s{2,}', ' ', tt_cleaned).strip()

                    if st != st_cleaned or tt != tt_cleaned:
                        timestamp_lines += 1
                        dirty_writer.writerow(row)
                    # After cleaning, write to clean file anyway.
                    writer.writerow(row)

                    pbar.update(1)

        return timestamp_lines

    except FileNotFoundError:
        print(f"Error: {input_file} not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


def remove_special_characters(input_file, output_file, dirty_file=None):
    """
    Removes special characters from the ST/TT column in the input file (.csv), and saves the cleaned content to the output file (.csv). Uncleaned contents will be saved to dirty_file (.csv).
    """
    # Counting lines...
    with open(input_file, 'r', encoding='utf-8') as f:
        total_rows = sum(1 for _ in f) - 1  # Exclude header line
    
    special_char_lines = 0
    try:
        with open(input_file, "r", encoding="utf-8") as infile, \
             open(output_file, "w", encoding="utf-8", newline='') as outfile, \
             open(dirty_file, "a", encoding="utf-8", newline='') if dirty_file else open('/dev/null', 'w') as dirty_outfile:
            reader = csv.DictReader(infile)
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            dirty_writer = csv.DictWriter(dirty_outfile, fieldnames=reader.fieldnames)
            writer.writeheader()

            with CommaTqdm(total=total_rows, desc="Cleaning special characters") as pbar:
                for row in reader:
                    st = re.sub(r'[\u200E\u202A\u202B\u202C\u202D\u202E\u2028\u2029\u00b6\u00a7\u0640\u200e]', '', row['st'])
                    tt = re.sub(r'[\u200E\u202A\u202B\u202C\u202D\u202E\u2028\u2029\u00b6\u00a7\u0640\u200e]', '', row['tt'])

                    # Replace various dash characters with standard hyphen-minus
                    # U+2013 = - = En Dash
                    # U+2014 = -- = Em Dash
                    st = re.sub(r'[\u2013\u2014]', '-', st)
                    tt = re.sub(r'[\u2013\u2014]', '-', tt)

                    # Replace various single quote characters with standard apostrophe
                    # U+2018 = ‘ = Left Single Quotation Mark
                    # U+2019 = ’ = Right Single Quotation Mark
                    # U+201A = ‚ = Single Low-9 Quotation Mark
                    # U+201B = ‛ = Single High-Reversed-9 Quotation Mark
                    # U+2032 = ′ = Prime
                    # U+2035 = ‵ = Reversed Prime
                    st = re.sub(r'[\u2018\u2019\u201A\u201B\u2032\u2035]', "'", st)
                    tt = re.sub(r'[\u2018\u2019\u201A\u201B\u2032\u2035]', "'", tt)

                    # Replace various double quote characters with standard quotation mark
                    # U+201C = “ = Left Double Quotation Mark
                    # U+201D = ” = Right Double Quotation Mark
                    # U+201E = „ = Double Low-9 Quotation Mark
                    # U+201F = ‟ = Double High-Reversed-9 Quotation Mark
                    # U+2033 = ″ = Double Prime
                    # U+2036 = ‶ = Reversed Double Prime
                    st = re.sub(r'[\u201C\u201D\u201E\u201F\u2033\u2036]', '"', st)
                    tt = re.sub(r'[\u201C\u201D\u201E\u201F\u2033\u2036]', '"', tt)

                    # Replace U+3000 (Ideographic Space) with standard space
                    st = st.replace('\u3000', ' ')
                    tt = tt.replace('\u3000', ' ')

                    if row['st'] != st or row['tt'] != tt:
                        dirty_writer.writerow(row)
                        special_char_lines += 1
                    # All fixed; write to clean file anyway.
                    writer.writerow(row)

                    pbar.update(1)

        return special_char_lines
    except FileNotFoundError:
        print(f"Error: {input_file} not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


def remove_duplicates(input_file, output_file, dirty_file=None):
    # Counting lines...
    with open(input_file, 'r', encoding='utf-8') as f:
        total_rows = sum(1 for _ in f) - 1  # Exclude header line

    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', newline='', encoding='utf-8') as outfile, \
         (open(dirty_file, 'a', newline='', encoding='utf-8') if dirty_file else open('/dev/null', 'w')) as dirty_outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        dirty_writer = csv.writer(dirty_outfile)
        
        duplicate_lines = 0

        with CommaTqdm(total=total_rows, desc="Removing duplicates") as pbar:
            for row in reader:
                if row[1] == row[2]:
                    dirty_writer.writerow(row)
                    duplicate_lines += 1
                else:
                    writer.writerow(row)

                pbar.update(1)

    return duplicate_lines


def remove_empty_lines(input_file, output_file, dirty_file=None):
    with open(input_file, 'r', encoding='utf-8') as infile:
        total_rows = sum(1 for _ in infile) -1  # Exclude header line
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', newline='', encoding='utf-8') as outfile, \
         (open(dirty_file, 'a', newline='', encoding='utf-8') if dirty_file else open('/dev/null', 'w')) as dirty_outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        dirty_writer = csv.writer(dirty_outfile)
        empty_lines = 0
        
        with CommaTqdm(total=total_rows, desc="Checking empty lines") as pbar:
            for row in reader:
                if row[1] != '' and row[2] != '':
                    writer.writerow(row)
                else:
                    dirty_writer.writerow(row)
                    empty_lines += 1

                pbar.update(1)

    return empty_lines


def main():
    parser = argparse.ArgumentParser(description="Merge bilingual files (pure texts) to CSV.")
    parser.add_argument("--st", required=True, help="Path to the ST file.")
    parser.add_argument("--tt", required=True, help="Path to the TT file.")
    parser.add_argument("--output", required=True, help="Path to the output merged CSV file.")
    parser.add_argument("--dirty", required=False, help="Path to the dirty file.")

    args = parser.parse_args()

    st_file = args.st
    tt_file = args.tt
    merged_file = args.output
    dirty_file = args.dirty
    tmp_file = "./data/tmp.csv"

    # 1. Merge bilingual files from pure text to CSV.
    total_rows = merge_bilingual_files(st_file, tt_file, tmp_file)
    shutil.copy(tmp_file, merged_file)
    print(f"Total ST & TT merged into CSV: {total_rows:,}")
    print()

    # 2. Convert Traditional Chinese to Simplified Chinese in the merged CSV.
    # new_clean_file = "./data/clean/01_zh_tw_to_zh_cn.csv"
    tc_number = convert_zh_tw_to_zh_cn(merged_file, tmp_file, dirty_file)
    shutil.copy(tmp_file, merged_file)
    print(f"Total Traditional Chinese lines converted: {tc_number:,}")
    print()

    # 3. Remove ST in TT from the merged CSV.
    bilingual_lines = remove_st_in_tt(merged_file, tmp_file, dirty_file)
    shutil.copy(tmp_file, merged_file)
    print(f"Total bilingual lines cleaned: {bilingual_lines:,}")
    print()

    # 4. Remove timestamps from the merged CSV.
    timestamp_lines = remove_timestamps(merged_file, tmp_file, dirty_file)
    shutil.copy(tmp_file, merged_file)
    print(f"Total timestamp lines cleaned: {timestamp_lines:,}")
    print()

    # 5. Remove special characters from the merged CSV.
    special_char_lines = remove_special_characters(merged_file, tmp_file, dirty_file)
    shutil.copy(tmp_file, merged_file)
    print(f"Total special character lines cleaned: {special_char_lines:,}")
    print()

    # 6. Remove duplicate contents from the merged CSV.
    duplicate_lines = remove_duplicates(merged_file, tmp_file, dirty_file)
    shutil.copy(tmp_file, merged_file)
    print(f"Total duplicate lines removed: {duplicate_lines:,}")
    print()

    # 7. Remove empty lines from the merged CSV.
    empty_lines_removed = remove_empty_lines(merged_file, tmp_file, dirty_file)
    shutil.move(tmp_file, merged_file)
    print(f"Total empty lines removed: {empty_lines_removed:,}")
    print()

    # Final report
    with open(merged_file, 'r', encoding='utf-8') as final_file:
        final_rows = sum(1 for _ in final_file) - 1  # Exclude header line
        processed_rows = tc_number + bilingual_lines + timestamp_lines + special_char_lines + duplicate_lines + empty_lines_removed
        print(f"Total processed lines: {processed_rows:,} / {total_rows:,} ({processed_rows/total_rows:.2%})")
        print(f"Final bilingual lines: {final_rows:,} / {total_rows:,} ({final_rows/total_rows:.2%})")
        print()


if __name__ == "__main__":
    main()