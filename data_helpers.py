import glob
import pandas as pd
import logging
import unicodedata
import re


def build_dataframe(input_csv_dir, resume, output_csv_path, logger_name, subset=None):
    """
    Reads and merges all csv files within input_csv_dir and returns a list of dictionaries of the data.
    :param input_csv_dir: string - path to the input directory where the csv files are located
    :param resume: bool - should the script resume where it stopped (resume=True) or start from scratch (resume=False)?
    :param output_csv_path: string - path to the output directory (useful if resume==True)
    :param logger_name: string - name of the logger loaded in the main file
    :param subset: int - number of rows to process (useful if the input dataset is huge)
    :return: list - list of dictionaries whose keys are the columns of the input csv files
    """
    logger = logging.getLogger(logger_name)
    logger.info("Loading csv files in {}.".format(input_csv_dir))

    data_files = glob.glob(str(input_csv_dir / '*.csv'))

    logger.info("Found {} csv files.".format(len(data_files)))

    dataframes = []
    for data_file in data_files:
        df_tmp = pd.read_csv(data_file, index_col='isrc')
        dataframes.append(df_tmp)

    df = pd.concat(dataframes, axis=0)

    if subset is not None:
        df = df.iloc[0:subset]

    if resume:
        try:
            df_output = pd.read_csv(output_csv_path, index_col="isrc")
            df = df.drop(df_output.index)
        except Exception as e:
            logger.error("Output file {} does not exist. Cannot resume.".format(output_csv_path))

    df = df.reset_index()
    list_of_dict = list(df.T.to_dict().values())
    return list_of_dict


def write_dataframe(list_of_dict, output_csv_path, resume, logger_name):
    """

    :param list_of_dict: list of dictionaries whose keys are the columns of the input csv files
    :param output_csv_path: string - path to the output directory
    :param resume: should the script resume where it stopped (resume=True) or start from scratch (resume=False)?
    :param logger_name:  string - name of the logger loaded in the main file
    :return: bool - True if the function ran successfully
    """
    logger = logging.getLogger(logger_name)

    df = pd.DataFrame(list_of_dict).set_index("isrc")
    if resume:
        try:
            dataframes = [df]
            df_output = pd.read_csv(output_csv_path, index_col="isrc")
            dataframes.append(df_output)
            df = pd.concat(dataframes, axis=0)
        except Exception as e:
            logger.error("Output file {} does not exist. Cannot resume.".format(output_csv_path))

    logger.info("Writing file {}.".format(output_csv_path))
    df.to_csv(output_csv_path, index=True)
    return True


def cjk_detect(texts):
    """
    Detects non latin characters within a string.
    :param texts: strin - the string to check.
    :return: string - string describing the type of character detected
    """
    # korean
    if re.search("[\uac00-\ud7a3]", texts):
        return "ko"
    # japanese
    if re.search("[\u3040-\u30ff]", texts):
        return "ja"
    # chinese
    if re.search("[\u4e00-\u9FFF]", texts):
        return "zh"
    # cyrillic
    if re.search('[\u0400-\u04FF]', texts):
        return "cy"
    # thai
    if re.search('[\u0E00-\u0E7F]', texts):
        return "th"
    return None


def remove_accents(s):
    """
    Removes all accents from a string. Works also for accent on non Latin characters.
    :param s: string - input string.
    :return: string - input string without any accent
    """
    return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn'))


def ct(input_str):
    """
    Simple string cleaning used to clean title (ct) of tracks.
    :param input_str: string - the title to be cleaned.
    :return: string - cleaned title
    """
    """Clean title (ct)."""
    clean_str = str(input_str)
    clean_str = re.sub("[\(\[].*?[\)\]]", " ", clean_str)
    clean_str = re.sub(' +', ' ', clean_str)  # remove double spaces
    clean_str = clean_str.strip()  # remove leading and trailing spaces
    return clean_str


def cs(input_str):
    """
    Clean String (cs).
    :param input_str: string - Input string to be cleaned.
    :return: clean_str: string - Output clean string.
    """
    clean_str = str(input_str)
    clean_str = clean_str.lower()
    clean_str = re.sub("[\(\[].*?[\)\]]", " ", clean_str)
    clean_str = clean_str.replace('[', ' ')
    clean_str = clean_str.replace(']', ' ')
    clean_str = clean_str.replace('(', ' ')
    clean_str = clean_str.replace(')', ' ')
    clean_str = re.sub("/", " ", clean_str)
    clean_str = re.sub('&', ' ', clean_str)
    clean_str = remove_accents(clean_str)
    clean_str = re.sub('featurering', '', clean_str)
    clean_str = re.sub('feature', '', clean_str)
    clean_str = re.sub('feat.', '', clean_str)
    clean_str = re.sub('ft.', '', clean_str)
    clean_str = re.sub('[^A-Za-z0-9 \uac00-\ud7a3\u3040-\u30ff\u4e00-\u9FFF\u0400-\u04FF\u0E00-\u0E7F]+', ' ', clean_str)
    clean_str = re.sub(' +', ' ', clean_str)  # remove double spaces
    clean_str = clean_str.strip()  # remove leading and trailing spaces

    return clean_str


def get_asymmetric_token_distance(str_short, str_long):
    """
    Custom calculation of some kind of normalized "distance" between a short and a long string. The distance will be
    zero if the short string is composed of tokens (a continuous sequence of characters with no space) that are all
    included in the longest string. If no token of the short string is included in the long string, the distance is 1.
    :param str_short: the shortest string
    :param str_long: the longest string
    :return: float - token distance
    """
    # Split the short string to get the list of tokens
    token_short = str_short.split()
    # Detect the one letter tokens
    to_be_joined = [x for x in token_short if len(x) == 1]
    # Remove the one letter tokens from the token list
    token_short = [x for x in token_short if x not in to_be_joined]
    # Add one token with the one letter tokens previously detected
    if len(to_be_joined) > 0:
        token_short.append(' '.join(to_be_joined))
    # Initialize the distance and the list of tokens present in both strings
    dist = 0
    str_best_list = []
    # Loop over the token list
    for token in token_short:
        # If the token is not found in the long string, increase the distance
        if not re.search(token, str_long):
            dist += 1/len(token_short)
        # The token is found in the long string, add it to the str_best_list list (for debugging)
        else:
            str_best_list.append(token)

    # Make a string from the tokens that were found in the long string (for debugging)
    str_best = ' '.join(str_best_list)

    return dist, str_best
