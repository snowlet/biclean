import csv
import re
from pbar import progress_bar
import time

def merge_bilingual_files(st_file, tt_file, output_file):
    """
    Merges two bilingual files (text) into one (.csv).
    The first file contains the source text (ST) and the second file contains the target text (TT).
    The output file will be a CSV with three columns: index, ST, TT.
    The first column is the index, the second column is the source text (ST),
    and the third column is the target text (TT).
    """
    try:
        with open(st_file, "r", encoding="utf-8") as stf, open(tt_file, "r", encoding="utf-8") as ttf, open(output_file, "w", encoding="utf-8", newline='') as outf:
            st_lines = stf.readlines()
            tt_lines = ttf.readlines()

            if len(st_lines) != len(tt_lines):
                print("Warning: The number of lines in ST and TT files do not match.")

            writer = csv.writer(outf)
            writer.writerow(["Index", "ST", "TT"])  # Write header

            # Convert TT lines to simplified Chinese,
            # as some contain traditional Chinese characters.
            for tt_line in tt_lines:
                tt_line = convert_chinese_variants(tt_line, target_variant="simplified", convert_idiom=False)

            for index, (st_line, tt_line) in enumerate(zip(st_lines, tt_lines), start=1):
                writer.writerow([index, st_line.strip(), tt_line.strip()])

        print(f"Merged file saved to {output_file}")
    except FileNotFoundError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


def remove_empty_lines(input_file, output_file, dirty_st_file=None, dirty_tt_file=None):
    """
    Removes empty lines from the ST and TT columns in the input file (.csv), and saves the cleaned content to the output file (.csv). Uncleaned contents will be saved to dirty_st_file and dirty_tt_file (.csv).
    Returns the numbers of lines removed.
    Columns of the .csv files: Index, ST, TT.
    """
    lines_st_removed = 0
    lines_tt_removed = 0
    lines_removed = 0
    try:
        with open(input_file, "r", encoding="utf-8") as infile, \
             open(output_file, "w", encoding="utf-8", newline='') as outfile, \
             open(dirty_st_file, "w", encoding="utf-8", newline='') if dirty_st_file else open('/dev/null', 'w') as dirty_st_outfile, \
             open(dirty_tt_file, "w", encoding="utf-8", newline='') if dirty_tt_file else open('/dev/null', 'w') as dirty_tt_outfile:
            reader = csv.DictReader(infile)
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            dirty_st_writer = csv.DictWriter(dirty_st_outfile, fieldnames=reader.fieldnames)
            dirty_tt_writer = csv.DictWriter(dirty_tt_outfile, fieldnames=reader.fieldnames)
            writer.writeheader()
            dirty_st_writer.writeheader()
            dirty_tt_writer.writeheader()

            for row in reader:
                if row["ST"].strip() == "":
                    lines_st_removed += 1
                    dirty_st_writer.writerow(row)
                    continue
                elif row["TT"].strip() == "":
                    lines_tt_removed += 1
                    dirty_tt_writer.writerow(row)
                    continue
                writer.writerow(row)

        lines_removed = lines_st_removed + lines_tt_removed

        print(f"Cleaned file saved to {output_file}. Dirty ST data ({lines_st_removed} lines) saved to {dirty_st_file}. Dirty TT data ({lines_tt_removed} lines) saved to {dirty_tt_file}. Total lines removed: {lines_removed}")
    except FileNotFoundError:
        print(f"Error: {input_file} not found.")
    except Exception as e:
        print(f"An error occurred: {e}")

    return lines_removed


def remove_st_in_tt(input_file, output_file, dirty_file=None):
    """
    Removes ST (from the second column) that exists at the end of the third column (TT).
    These rows imply bilingual content and only the ST are cleaned from the TT.
    The cleaned content is saved to output_file, and the bilingual contents are saved to dirty_file.
    Returns the number of bilingual lines.
    """
    bilingual_lines = 0
    try:
        with open(input_file, "r", encoding="utf-8") as infile, \
             open(output_file, "w", encoding="utf-8", newline='') as outfile, \
             open(dirty_file, "w", encoding="utf-8", newline='') if dirty_file else open('/dev/null', 'w') as dirty_outfile:
            reader = csv.DictReader(infile)
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            dirty_writer = csv.DictWriter(dirty_outfile, fieldnames=reader.fieldnames)
            writer.writeheader()
            dirty_writer.writeheader()

            for row in reader:
                st = row["ST"].strip()
                tt = row["TT"].strip()
                if tt.endswith(st):
                    bilingual_lines += 1
                    dirty_writer.writerow(row)
                    row["TT"] = tt[:-len(st)].strip()
                writer.writerow(row)
        print(f"Cleaned file saved to {output_file}. Bilingual data ({bilingual_lines} lines) saved to {dirty_file}.")
        return bilingual_lines
    except FileNotFoundError:
        print(f"Error: {input_file} not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


def remove_ass_tags(input_file, output_file, dirty_file=None):
    """
    Removes .ass tags from the ST/TT column in the input file (.csv), and saves the cleaned content to the output file (.csv). Uncleaned contents will be saved to dirty_file (.csv).
    .ass tags are quoted by curly brackets, such as {\fs20}, {\fnxxx}, {\an8}.
    Returns the number of lines with ass tags removed.
    Columns of the .csv files: Index, ST, TT.
    """
    time_tag_lines = 0
    try:
        with open(input_file, "r", encoding="utf-8") as infile, \
             open(output_file, "w", encoding="utf-8", newline='') as outfile, \
             open(dirty_file, "w", encoding="utf-8", newline='') if dirty_file else open('/dev/null', 'w') as dirty_outfile:
            reader = csv.DictReader(infile)
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            dirty_writer = csv.DictWriter(dirty_outfile, fieldnames=reader.fieldnames)
            writer.writeheader()
            dirty_writer.writeheader()

            for row in reader:
                st = row["ST"].strip()
                tt = row["TT"].strip()

                # Remove .ass tags from ST
                st_cleaned = remove_curly_bracket_tags(st)
                tt_cleaned = remove_curly_bracket_tags(tt)
                if st != st_cleaned or tt != tt_cleaned:
                    time_tag_lines += 1
                    dirty_writer.writerow(row)
                else:
                    writer.writerow(row)

        print(f"Cleaned file saved to {output_file}. Dirty data ({time_tag_lines} lines) saved to {dirty_file}.")
        return time_tag_lines

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

    time_tag_lines = 0
    try:
        with open(input_file, "r", encoding="utf-8") as infile, \
             open(output_file, "w", encoding="utf-8", newline='') as outfile, \
             open(dirty_file, "w", encoding="utf-8", newline='') if dirty_file else open('/dev/null', 'w') as dirty_outfile:
            reader = csv.DictReader(infile)
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            dirty_writer = csv.DictWriter(dirty_outfile, fieldnames=reader.fieldnames)
            writer.writeheader()
            dirty_writer.writeheader()

            for row in reader:
                st = row["ST"].strip()
                tt = row["TT"].strip()
                for timestamp_pattern in timestamp_patterns:
                    st_cleaned = timestamp_pattern.sub("", st)
                    tt_cleaned = timestamp_pattern.sub("", tt)
                    st_cleaned = re.sub(r'\s{2,}', ' ', st_cleaned).strip()
                    tt_cleaned = re.sub(r'\s{2,}', ' ', tt_cleaned).strip()

                if st != st_cleaned or tt != tt_cleaned:
                    time_tag_lines += 1
                    dirty_writer.writerow(row)
                else:
                    writer.writerow(row)

        print(f"Cleaned file saved to {output_file}. Dirty data ({time_tag_lines} lines) saved to {dirty_file}.")
        return time_tag_lines

    except FileNotFoundError:
        print(f"Error: {input_file} not found.")
    except Exception as e:
        print(f"An error occurred: {e}")
 

def remove_curly_bracket_tags(text):
    """
    Removes .ass tags from the given text.
    Anything inside the curly bracket should be removed. The curly brackets should be removed, too.
    """
    return re.sub(r"\{.*?\}", "", text)


def remove_square_bracket_tags(text):
    """
    Removes .ass tags from the given text.
    Anything inside the curly bracket should be removed. The curly brackets should be removed, too.
    """
    return re.sub(r"\[.*?\]", "", text)


def count_lines(path: str) -> int:
    """
    Counts the number of lines in a file.
    """
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return sum(1 for _ in f)
   

def pre_process_file(input_file, output_file):
    """
    Processes the input file by removing special characters and saves the cleaned content to the output file.
    """
    try:
        with open(input_file, "r", encoding="utf-8") as infile, open(output_file, "w", encoding="utf-8") as outfile:
            for line in infile:
                cleaned_line = remove_special_chars(line)
                outfile.write(cleaned_line)
        print(f"Processed file saved to {output_file}")
    except FileNotFoundError:
        print(f"Error: {input_file} not found.")
    except Exception as e:
        print(f"An error occurred: {e}")


def remove_duplicate_content(input_file, output_file_clean, output_file_duplicate):
    """
    Removes rows where the third column (TT) is the same as the second column (ST) and writes them to separate files.
    """

    data_total = count_lines(input_file) - 1 # exclude header
    data_idx = 1
    start_time = time.time()

    print("Removing duplicate content...")

    with open(input_file, mode='r', encoding='utf-8') as infile, \
        open(output_file_clean, mode='w', encoding='utf-8', newline='') as outfile_1, \
        open(output_file_duplicate, mode='w', encoding='utf-8', newline='') as outfile_2:
        reader = csv.DictReader(infile)
        writer_clean = csv.DictWriter(outfile_1, fieldnames=reader.fieldnames)
        writer_duplicate = csv.DictWriter(outfile_2, fieldnames=reader.fieldnames)
        writer_clean.writeheader()
        writer_duplicate.writeheader()

        for row in reader:
            progress_bar(data_idx, data_total, start_time=start_time)
            if row[reader.fieldnames[2]] == row[reader.fieldnames[1]]:
                writer_duplicate.writerow(row)
            else:
                writer_clean.writerow(row)
            data_idx += 1


def remove_bilingual_content(input_file, output_file_clean, output_file_bilingual):
    """
    Removes ST (from the second column) that exists at the end of the third column (TT).
    These rows imply bilingual content and are cleaned from the TT.
    """

    data_total = count_lines(input_file) - 1 # exclude header
    data_idx = 1
    start_time = time.time()

    print("Removing bilingual content...")

    with open(input_file, mode='r', encoding='utf-8') as infile, \
        open(output_file_clean, mode='w', encoding='utf-8', newline='') as outfile_clean, \
        open(output_file_bilingual, mode='w', encoding='utf-8', newline='') as outfile_bilingual:
        reader = csv.DictReader(infile)
        writer_clean = csv.DictWriter(outfile_clean, fieldnames=reader.fieldnames)
        writer_bilingual = csv.DictWriter(outfile_bilingual, fieldnames=reader.fieldnames)
        writer_clean.writeheader()
        writer_bilingual.writeheader()

        for row in reader:
            progress_bar(data_idx, data_total, start_time=start_time)
            col2 = row[reader.fieldnames[1]]
            col3 = row[reader.fieldnames[2]]
            if col3.endswith(col2):
                writer_bilingual.writerow(row)
                row[reader.fieldnames[2]] = col3[:-len(col2)]
                writer_clean.writerow(row)
            else:
                writer_clean.writerow(row)
            data_idx += 1


def remove_special_chars(text):
    """
    Removes specified special characters from a given text.
    """
    # Removes special characters such as U+200E, U+202A, U+202C from the fields.
    # U+200E = Left-to-Right Mark (LRM)
    # U+202A = Left-to-Right Embedding (LRE)
    # U+202B = Right-to-Left Embedding (RLE)
    # U+202C = Pop Directional Formatting (PDF)
    # U+202D = Left-to-Right Override (LRO)
    # U+202E = Right-to-Left Override (RLO)
    chars = ["\u200e", "\u202a", "\u202b", "\u202c", "\u202d", "\u202e", "lrm;", "lre;", "rle;", "pdf;", "lro;", "rlo;"]
    for char in chars:
        text = text.replace(char, "")

    # Remove other unwanted characters
    # U+2018 = ' = Left Single Quotation Mark
    # U+2019 = ' = Right Single Quotation Mark
    # U+201A = ' = Single Low-9 Quotation Mark
    # U+201B = ' = Single High-Reversed-9 Quotation Mark
    # U+2032 = ' = Prime
    # U+2035 = ' = Reversed Prime
    chars = ["\u2018", "\u2019", "\u201a", "\u201b", "\u2032", "\u2035"]
    for char in chars:
        text = text.replace(char, "'")

    # U+201C = " = Left Double Quotation Mark
    # U+201D = " = Right Double Quotation Mark
    # U+201E = " = Double Low-9 Quotation Mark
    # U+201F = " = Double High-Reversed-9 Quotation Mark
    # U+2033 = " = Double Prime
    # U+2036 = " = Reversed Double Prime
    chars = ["\u201c", "\u201d", "\u201e", "\u201f", "\u2033", "\u2036"]
    for char in chars:
        text = text.replace(char, '"')
    
    # U+2013 = - = En Dash
    text = text.replace("\u2013", "-")
    
    # U+2014 = -- = Em Dash
    text = text.replace("\u2014", "-")

    # U+2026 = ... = Horizontal Ellipsis
    # text = text.replace("\u2026", "...")
    
    # U+2028 = \n = Line Separator
    # U+2029 = \n = Paragraph Separator
    chars = ["\u2028", "\u2029"]
    for char in chars:
        text = text.replace(char, "\n")
    
    # U+00B6 = ¶ = Pilcrow Sign
    # U+00A7 = § = Section Sign
    text = text.replace("\u00b6", "")
    text = text.replace("\u00a7", "")

    # U+0640 = ـ = Arabic Tatweel
    text = text.replace("\u0640", "")

    # U+3000 =  = Ideographic Space
    # text = text.replace("\u3000", " ")

    return text


def convert_chinese_variants(text, target_variant='simplified', convert_idiom=False):
    """
    Converts Chinese text between Simplified and Traditional variants.
    target_variant: 'simplified' or 'traditional'
    convert_idiom: whether to convert idioms
    """
    try:
        from opencc import OpenCC
    except ImportError:
        print("Error: opencc module not found. Please install it using 'pip install opencc-python-reimplemented'.")
        return text

    if target_variant == 'simplified':
        cc = OpenCC('tw2sp.json' if convert_idiom else 't2s.json')
    elif target_variant == 'traditional':
        cc = OpenCC('s2twp.json' if convert_idiom else 's2t.json')
    else:
        print("Error: target_variant must be either 'simplified' or 'traditional'.")
        return text

    converted_text = cc.convert(text)
    return converted_text

def remove_non_cjk_characters(text):
    """
    Removes non-CJK characters from the given text.
    """
    cleaned_text = ''.join(char for char in text if is_cjk_character(char))
    return cleaned_text


def is_cjk_character(char: str) -> bool:
    """
    Checks if a character is a CJK character.
    """
    return any([
        '\u4E00' <= char <= '\u9FFF',  # CJK Unified Ideographs
        '\u3400' <= char <= '\u4DBF',  # CJK Unified Ideographs Extension A
        '\u20000' <= char <= '\u2A6DF',  # CJK Unified Ideographs Extension B
        '\u2A700' <= char <= '\u2B73F',  # CJK Unified Ideographs Extension C
        '\u2B740' <= char <= '\u2B81F',  # CJK Unified Ideographs Extension D
        '\u2B820' <= char <= '\u2CEAF',  # CJK Unified Ideographs Extension E
        '\uF900' <= char <= '\uFAFF',  # CJK Compatibility Ideographs
        '\u2F800' <= char <= '\u2FA1F',  # CJK Compatibility Ideographs Supplement
        '\u3040' <= char <= '\u309F',  # Hiragana
        '\u30A0' <= char <= '\u30FF',  # Katakana
        '\u31F0' <= char <= '\u31FF',  # Katakana Phonetic Extensions
        '\uAC00' <= char <= '\uD7AF',  # Hangul Syllables
        '\u1100' <= char <= '\u11FF',  # Hangul Jamo
        '\u3130' <= char <= '\u318F',  # Hangul Compatibility Jamo
    ])


def if_contain_cjk(text):
    """
    Checks if the text contains any CJK characters.
    """
    return any(is_cjk_character(char) for char in text)


def main():
    """
    Main function to execute the script.
    """
    # Example usage
    # Both st_file and tt_file below are examples from OpenSubtitles2024 dataset.
    st_file = "./data/raw/OpenSubtitles.en-zh_CN.en-100k"
    tt_file = "./data/raw/OpenSubtitles.en-zh_CN.zh_CN-100k"
    merged_file = "./data/raw/merged_raw.csv"
    cleaned_file = "cleaned.csv"
    duplicate_file = "duplicates.csv"
    bilingual_file = "bilingual.csv"

    merge_bilingual_files(st_file, tt_file, merged_file)

    remove_empty_lines(merged_file, "./data/clean/01_cleaned.csv", "./data/dirty/st_dirty.csv", "./data/dirty/tt_dirty.csv")

    remove_st_in_tt("./data/clean/01_cleaned.csv", "./data/clean/02_st_in _tt_removed.csv", "./data/dirty/bilingual_dirty.csv")

    remove_ass_tags("./data/clean/02_st_in _tt_removed.csv", "./data/clean/03_ass_tags_removed.csv", "./data/dirty/ass_tags_dirty.csv")

    remove_timestamps("./data/clean/03_ass_tags_removed.csv", "./data/clean/04_time_tags_removed.csv", "./data/dirty/time_tags_dirty.csv")

    #pre_process_file(merged_file, "preprocessed.csv")
    #remove_duplicate_content("preprocessed.csv", cleaned_file, duplicate_file)
    #remove_bilingual_content(cleaned_file, "final_cleaned.csv", bilingual_file)

if __name__ == "__main__":
    main()
