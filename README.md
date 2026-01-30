# biclean
Specific-purpose tools for cleaning noisy bilingual subtitle corpora (currently focused on EN–ZH).

## Description
This repository contains tools for cleaning noisy bilingual corpora, with a focus on English and Chinese subtitles. The tools provide functionality for identifying and removing noisy data, such as time codes, html-like tags, and other issues that can negatively impact the quality of translation and translation studies.

## Files
- `bi_clean.py`:    Main script for cleaning bilingual corpora.
- `data/`:          Directory containing example data sets for testing the cleaning tools.

## Installation
To use the biclean cleaning tools, you will need to install the following dependencies:

- Python 3.6 or later

You can install the dependencies using pip:

`python -m pip install csv tqdm argparse re opencc-python-reimplemented`

## Usage
Usage: `python biclean.py [-h] --st ST --tt TT --output OUTPUT [--dirty DIRTY]`

Merge bilingual files (pure texts) to CSV.

Options:
  `-h`, `--help`     show this help message and exit
  `--st ST`          Path to the ST file.
  `--tt TT`          Path to the TT file.
  `--output OUTPUT`  Path to the output merged CSV file.
  `--dirty DIRTY`    Path to the dirty file.
