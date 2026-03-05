import csv
from tqdm import tqdm
import argparse
import re
import shutil
import os
import itertools
from multiprocessing import Pool

# Chunk size for parallel processing (~50k–100k rows). Tune for 22M rows.
CHUNK_SIZE = 100_000


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


def _read_csv_chunks_dict(input_file, chunk_size):
    """Yield (fieldnames, chunk) for each chunk of dict rows. Caller must open/close file or use in with block."""
    with open(input_file, 'r', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        while True:
            chunk = list(itertools.islice(reader, chunk_size))
            if not chunk:
                break
            yield (fieldnames, chunk)


def _read_csv_chunks_raw(input_file, chunk_size):
    """Yield (header, chunk) once for header, then (None, chunk) for each chunk of list rows."""
    with open(input_file, 'r', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        header = next(reader)
        while True:
            chunk = list(itertools.islice(reader, chunk_size))
            if not chunk:
                break
            yield (header, chunk)


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


def _worker_convert_zh_tw_to_zh_cn(chunk):
    """Process one chunk: convert TT to simplified Chinese. Returns (cleaned_rows, dirty_rows)."""
    from opencc import OpenCC
    converter = OpenCC('t2s')
    cleaned = []
    dirty = []
    for row in chunk:
        tt_content = converter.convert(row["tt"])
        if row["tt"] != tt_content:
            dirty.append({"index": row["index"], "st": row["st"], "tt": row["tt"]})
        cleaned.append({"index": row["index"], "st": row["st"], "tt": tt_content})
    return (cleaned, dirty)


def convert_zh_tw_to_zh_cn(input_file, output_file, dirty_file=None, workers=1):
    with open(input_file, 'r', encoding='utf-8') as f:
        total_rows = sum(1 for _ in f) - 1  # Exclude header line

    chunks_iter = _read_csv_chunks_dict(input_file, CHUNK_SIZE)
    first = next(chunks_iter, None)
    if first is None:
        return 0
    fieldnames, first_chunk = first

    def chunk_gen():
        yield first_chunk
        for _, c in chunks_iter:
            yield c

    with open(output_file, 'w', encoding='utf-8', newline='') as outfile, \
         open(dirty_file, 'a', encoding='utf-8', newline='') if dirty_file else open('/dev/null', 'w') as dirty_outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        dirty_writer = csv.DictWriter(dirty_outfile, fieldnames=fieldnames)
        writer.writeheader()
        dirty_writer.writeheader()
        tc_number = 0

        if workers <= 1:
            with CommaTqdm(total=total_rows, desc="Converting zh-TW to zh-CN") as pbar:
                for chunk in chunk_gen():
                    cleaned, dirty = _worker_convert_zh_tw_to_zh_cn(chunk)
                    for r in cleaned:
                        writer.writerow(r)
                    for r in dirty:
                        dirty_writer.writerow(r)
                        tc_number += 1
                    pbar.update(len(chunk))
        else:
            with CommaTqdm(total=total_rows, desc="Converting zh-TW to zh-CN") as pbar, \
                 Pool(workers) as pool:
                for cleaned, dirty in pool.imap(_worker_convert_zh_tw_to_zh_cn, chunk_gen()):
                    for r in cleaned:
                        writer.writerow(r)
                    for r in dirty:
                        dirty_writer.writerow(r)
                        tc_number += 1
                    pbar.update(len(cleaned))

    return tc_number


def _worker_remove_st_in_tt(chunk):
    """Remove ST from TT in chunk. Returns (cleaned_rows, dirty_rows)."""
    cleaned = []
    dirty = []
    for row in chunk:
        st = row["st"].strip()
        tt = row["tt"].strip()
        out = dict(row)
        if tt.endswith(st):
            dirty.append(row)
            out["tt"] = tt[:-len(st)].strip()
        elif tt.startswith(st):
            dirty.append(row)
            out["tt"] = tt[len(st):].strip()
        cleaned.append(out)
    return (cleaned, dirty)


def remove_st_in_tt(input_file, output_file, dirty_file=None, workers=1):
    """
    Removes ST (from the second column) that exists at the end of the third column (TT).
    Returns the number of bilingual lines.
    """
    with open(input_file, 'r', encoding='utf-8') as f:
        total_rows = sum(1 for _ in f) - 1  # Exclude header line

    chunks_iter = _read_csv_chunks_dict(input_file, CHUNK_SIZE)
    first = next(chunks_iter, None)
    if first is None:
        return 0
    fieldnames, first_chunk = first

    def chunk_gen():
        yield first_chunk
        for _, c in chunks_iter:
            yield c

    bilingual_lines = 0
    try:
        with open(output_file, "w", encoding="utf-8", newline='') as outfile, \
             open(dirty_file, "w", encoding="utf-8", newline='') if dirty_file else open('/dev/null', 'w') as dirty_outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            dirty_writer = csv.DictWriter(dirty_outfile, fieldnames=fieldnames)
            writer.writeheader()

            if workers <= 1:
                with CommaTqdm(total=total_rows, desc="Removing ST from TT") as pbar:
                    for chunk in chunk_gen():
                        cleaned, dirty = _worker_remove_st_in_tt(chunk)
                        for r in cleaned:
                            writer.writerow(r)
                        for r in dirty:
                            dirty_writer.writerow(r)
                            bilingual_lines += 1
                        pbar.update(len(chunk))
            else:
                with CommaTqdm(total=total_rows, desc="Removing ST from TT") as pbar, \
                     Pool(workers) as pool:
                    for cleaned, dirty in pool.imap(_worker_remove_st_in_tt, chunk_gen()):
                        for r in cleaned:
                            writer.writerow(r)
                        for r in dirty:
                            dirty_writer.writerow(r)
                            bilingual_lines += 1
                        pbar.update(len(cleaned))
        return bilingual_lines
    except FileNotFoundError:
        print(f"Error: {input_file} not found.")
        return 0
    except Exception as e:
        print(f"An error occurred: {e}")
        return 0


def _timestamp_patterns():
    """Build list of compiled timestamp regexes (used in worker)."""
    patterns = []
    patterns.append(re.compile(
        r'\b\d+\s+' r'\d{2}:\d{2}:\d{2}[,\.]\d{3}' r'\s+--\s+' r'\d{2}:\d{2}:\d{2}[,\.]\d{3}'))
    patterns.append(re.compile(
        r'\d{2}:\d{2}:\d{2}[,\.]\d{3}' r'\.+\s+' r'\d{2}:\d{2}:\d{2}[,\.]\d{3}'))
    patterns.append(re.compile(r'\b[A-Za-z]+:\s*\d+,\d+:\d{2}:\d{2}[.,]\d{2,3}\.\.\.'))
    patterns.append(re.compile(r'\b\d+\s+' r'\d{2}:\d{2}:\d{2}[,\.]\d{3}'))
    patterns.append(re.compile(r'\[(\d{2}:\d{2}:\d{2}[.,]\d{2})\]'))
    return patterns


def _worker_remove_timestamps(chunk):
    """Remove timestamp patterns from ST/TT. Returns (cleaned_rows, dirty_rows)."""
    timestamp_patterns = _timestamp_patterns()
    cleaned = []
    dirty = []
    for row in chunk:
        st = row["st"].strip()
        tt = row["tt"].strip()
        st_cleaned, tt_cleaned = st, tt
        for pat in timestamp_patterns:
            st_cleaned = pat.sub("", st_cleaned)
            tt_cleaned = pat.sub("", tt_cleaned)
            st_cleaned = re.sub(r'\s{2,}', ' ', st_cleaned).strip()
            tt_cleaned = re.sub(r'\s{2,}', ' ', tt_cleaned).strip()
        out = {"index": row["index"], "st": st_cleaned, "tt": tt_cleaned}
        if st != st_cleaned or tt != tt_cleaned:
            dirty.append(row)
        cleaned.append(out)
    return (cleaned, dirty)


def remove_timestamps(input_file, output_file, dirty_file=None, workers=1):
    """
    Removes .ass/timestamp tags from ST/TT. Uncleaned contents saved to dirty_file.
    Returns the number of lines with time tags removed.
    """
    with open(input_file, 'r', encoding='utf-8') as f:
        total_rows = sum(1 for _ in f) - 1  # Exclude header line

    chunks_iter = _read_csv_chunks_dict(input_file, CHUNK_SIZE)
    first = next(chunks_iter, None)
    if first is None:
        return 0
    fieldnames, first_chunk = first

    def chunk_gen():
        yield first_chunk
        for _, c in chunks_iter:
            yield c

    timestamp_lines = 0
    try:
        with open(output_file, "w", encoding="utf-8", newline='') as outfile, \
             open(dirty_file, "a", encoding="utf-8", newline='') if dirty_file else open('/dev/null', 'w') as dirty_outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            dirty_writer = csv.DictWriter(dirty_outfile, fieldnames=fieldnames)
            writer.writeheader()

            if workers <= 1:
                with CommaTqdm(total=total_rows, desc="Cleaning timestamps from ST & TT") as pbar:
                    for chunk in chunk_gen():
                        cleaned, dirty = _worker_remove_timestamps(chunk)
                        for r in cleaned:
                            writer.writerow(r)
                        for r in dirty:
                            dirty_writer.writerow(r)
                            timestamp_lines += 1
                        pbar.update(len(chunk))
            else:
                with CommaTqdm(total=total_rows, desc="Cleaning timestamps from ST & TT") as pbar, \
                     Pool(workers) as pool:
                    for cleaned, dirty in pool.imap(_worker_remove_timestamps, chunk_gen()):
                        for r in cleaned:
                            writer.writerow(r)
                        for r in dirty:
                            dirty_writer.writerow(r)
                            timestamp_lines += 1
                        pbar.update(len(cleaned))
        return timestamp_lines
    except FileNotFoundError:
        print(f"Error: {input_file} not found.")
        return 0
    except Exception as e:
        print(f"An error occurred: {e}")
        return 0


def _worker_remove_special_characters(chunk):
    """Normalize special characters in ST/TT. Returns (cleaned_rows, dirty_rows)."""
    cleaned = []
    dirty = []
    for row in chunk:
        st = re.sub(r'[\u200E\u202A\u202B\u202C\u202D\u202E\u2028\u2029\u00b6\u00a7\u0640\u200e]', '', row['st'])
        tt = re.sub(r'[\u200E\u202A\u202B\u202C\u202D\u202E\u2028\u2029\u00b6\u00a7\u0640\u200e]', '', row['tt'])
        st = re.sub(r'[\u2013\u2014]', '-', st)
        tt = re.sub(r'[\u2013\u2014]', '-', tt)
        st = re.sub(r'[\u2018\u2019\u201A\u201B\u2032\u2035]', "'", st)
        tt = re.sub(r'[\u2018\u2019\u201A\u201B\u2032\u2035]', "'", tt)
        st = re.sub(r'[\u201C\u201D\u201E\u201F\u2033\u2036]', '"', st)
        tt = re.sub(r'[\u201C\u201D\u201E\u201F\u2033\u2036]', '"', tt)
        st = st.replace('\u3000', ' ')
        tt = tt.replace('\u3000', ' ')
        out = {"index": row["index"], "st": st, "tt": tt}
        if row['st'] != st or row['tt'] != tt:
            dirty.append(row)
        cleaned.append(out)
    return (cleaned, dirty)


def remove_special_characters(input_file, output_file, dirty_file=None, workers=1):
    """
    Removes special characters from ST/TT. Uncleaned contents saved to dirty_file.
    """
    with open(input_file, 'r', encoding='utf-8') as f:
        total_rows = sum(1 for _ in f) - 1  # Exclude header line

    chunks_iter = _read_csv_chunks_dict(input_file, CHUNK_SIZE)
    first = next(chunks_iter, None)
    if first is None:
        return 0
    fieldnames, first_chunk = first

    def chunk_gen():
        yield first_chunk
        for _, c in chunks_iter:
            yield c

    special_char_lines = 0
    try:
        with open(output_file, "w", encoding="utf-8", newline='') as outfile, \
             open(dirty_file, "a", encoding="utf-8", newline='') if dirty_file else open('/dev/null', 'w') as dirty_outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            dirty_writer = csv.DictWriter(dirty_outfile, fieldnames=fieldnames)
            writer.writeheader()

            if workers <= 1:
                with CommaTqdm(total=total_rows, desc="Cleaning special characters") as pbar:
                    for chunk in chunk_gen():
                        cleaned, dirty = _worker_remove_special_characters(chunk)
                        for r in cleaned:
                            writer.writerow(r)
                        for r in dirty:
                            dirty_writer.writerow(r)
                            special_char_lines += 1
                        pbar.update(len(chunk))
            else:
                with CommaTqdm(total=total_rows, desc="Cleaning special characters") as pbar, \
                     Pool(workers) as pool:
                    for cleaned, dirty in pool.imap(_worker_remove_special_characters, chunk_gen()):
                        for r in cleaned:
                            writer.writerow(r)
                        for r in dirty:
                            dirty_writer.writerow(r)
                            special_char_lines += 1
                        pbar.update(len(cleaned))
        return special_char_lines
    except FileNotFoundError:
        print(f"Error: {input_file} not found.")
        return 0
    except Exception as e:
        print(f"An error occurred: {e}")
        return 0


def _worker_remove_duplicates(chunk):
    """Split chunk into non-duplicate (cleaned) and duplicate (dirty) rows. Each row is [index, st, tt]."""
    cleaned = []
    dirty = []
    for row in chunk:
        if row[1] == row[2]:
            dirty.append(row)
        else:
            cleaned.append(row)
    return (cleaned, dirty)


def remove_duplicates(input_file, output_file, dirty_file=None, workers=1):
    with open(input_file, 'r', encoding='utf-8') as f:
        total_rows = sum(1 for _ in f) - 1  # Exclude header line

    chunks_iter = _read_csv_chunks_raw(input_file, CHUNK_SIZE)
    first = next(chunks_iter, None)
    if first is None:
        return 0
    header, first_chunk = first

    def chunk_gen():
        yield first_chunk
        for _, c in chunks_iter:
            yield c

    with open(output_file, 'w', newline='', encoding='utf-8') as outfile, \
         (open(dirty_file, 'a', newline='', encoding='utf-8') if dirty_file else open('/dev/null', 'w')) as dirty_outfile:
        writer = csv.writer(outfile)
        dirty_writer = csv.writer(dirty_outfile)
        writer.writerow(header)
        duplicate_lines = 0

        if workers <= 1:
            with CommaTqdm(total=total_rows, desc="Removing duplicates") as pbar:
                for chunk in chunk_gen():
                    cleaned, dirty = _worker_remove_duplicates(chunk)
                    for r in cleaned:
                        writer.writerow(r)
                    for r in dirty:
                        dirty_writer.writerow(r)
                        duplicate_lines += 1
                    pbar.update(len(chunk))
        else:
            with CommaTqdm(total=total_rows, desc="Removing duplicates") as pbar, \
                 Pool(workers) as pool:
                for cleaned, dirty in pool.imap(_worker_remove_duplicates, chunk_gen()):
                    for r in cleaned:
                        writer.writerow(r)
                    for r in dirty:
                        dirty_writer.writerow(r)
                        duplicate_lines += 1
                    pbar.update(len(cleaned) + len(dirty))

    return duplicate_lines


def _worker_remove_empty_lines(chunk):
    """Split chunk into non-empty (cleaned) and empty (dirty) rows."""
    cleaned = []
    dirty = []
    for row in chunk:
        if row[1] != '' and row[2] != '':
            cleaned.append(row)
        else:
            dirty.append(row)
    return (cleaned, dirty)


def remove_empty_lines(input_file, output_file, dirty_file=None, workers=1):
    with open(input_file, 'r', encoding='utf-8') as infile:
        total_rows = sum(1 for _ in infile) - 1  # Exclude header line

    chunks_iter = _read_csv_chunks_raw(input_file, CHUNK_SIZE)
    first = next(chunks_iter, None)
    if first is None:
        return 0
    header, first_chunk = first

    def chunk_gen():
        yield first_chunk
        for _, c in chunks_iter:
            yield c

    with open(output_file, 'w', newline='', encoding='utf-8') as outfile, \
         (open(dirty_file, 'a', newline='', encoding='utf-8') if dirty_file else open('/dev/null', 'w')) as dirty_outfile:
        writer = csv.writer(outfile)
        dirty_writer = csv.writer(dirty_outfile)
        writer.writerow(header)
        empty_lines = 0

        if workers <= 1:
            with CommaTqdm(total=total_rows, desc="Checking empty lines") as pbar:
                for chunk in chunk_gen():
                    cleaned, dirty = _worker_remove_empty_lines(chunk)
                    for r in cleaned:
                        writer.writerow(r)
                    for r in dirty:
                        dirty_writer.writerow(r)
                        empty_lines += 1
                    pbar.update(len(chunk))
        else:
            with CommaTqdm(total=total_rows, desc="Checking empty lines") as pbar, \
                 Pool(workers) as pool:
                for cleaned, dirty in pool.imap(_worker_remove_empty_lines, chunk_gen()):
                    for r in cleaned:
                        writer.writerow(r)
                    for r in dirty:
                        dirty_writer.writerow(r)
                        empty_lines += 1
                    pbar.update(len(cleaned) + len(dirty))

    return empty_lines


def main():
    parser = argparse.ArgumentParser(description="Merge bilingual files (pure texts) to CSV.")
    parser.add_argument("--st", required=True, help="Path to the ST file.")
    parser.add_argument("--tt", required=True, help="Path to the TT file.")
    parser.add_argument("--output", required=True, help="Path to the output merged CSV file.")
    parser.add_argument("--dirty", required=False, help="Path to the dirty file.")
    parser.add_argument("--workers", type=int, default=None,
                        help="Number of worker processes for CPU-bound steps (default: CPU count). Use 1 for single-process.")

    args = parser.parse_args()
    workers = args.workers if args.workers is not None else (os.cpu_count() or 4)

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
    tc_number = convert_zh_tw_to_zh_cn(merged_file, tmp_file, dirty_file, workers=workers)
    shutil.copy(tmp_file, merged_file)
    print(f"Total Traditional Chinese lines converted: {tc_number:,}")
    print()

    # 3. Remove ST in TT from the merged CSV.
    bilingual_lines = remove_st_in_tt(merged_file, tmp_file, dirty_file, workers=workers)
    shutil.copy(tmp_file, merged_file)
    print(f"Total bilingual lines cleaned: {bilingual_lines:,}")
    print()

    # 4. Remove timestamps from the merged CSV.
    timestamp_lines = remove_timestamps(merged_file, tmp_file, dirty_file, workers=workers)
    shutil.copy(tmp_file, merged_file)
    print(f"Total timestamp lines cleaned: {timestamp_lines:,}")
    print()

    # 5. Remove special characters from the merged CSV.
    special_char_lines = remove_special_characters(merged_file, tmp_file, dirty_file, workers=workers)
    shutil.copy(tmp_file, merged_file)
    print(f"Total special character lines cleaned: {special_char_lines:,}")
    print()

    # 6. Remove duplicate contents from the merged CSV.
    duplicate_lines = remove_duplicates(merged_file, tmp_file, dirty_file)
    shutil.copy(tmp_file, merged_file)
    print(f"Total duplicate lines removed: {duplicate_lines:,}")
    print()

    # 7. Remove empty lines from the merged CSV.
    empty_lines_removed = remove_empty_lines(merged_file, tmp_file, dirty_file, workers=workers)
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