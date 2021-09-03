import logging
import traceback
import youtube_dl
from youtube_search import YoutubeSearch
from constants import youtube_url_prefix, YOUTUBE_LINK_EXTRACTOR_LOGGER
from data_helpers import cs, ct, get_asymmetric_token_distance
from multiprocessing import Lock
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(YOUTUBE_LINK_EXTRACTOR_LOGGER)

lock = Lock()
count = 0
total = 0
succeeded = 0
failed = 0


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=2, min=4, max=20))
def extract_youtube_link(track):
    """
    Find and extract the YouTube link corresponding to the input track.
    :param track: dict - dictionary of the track containing the three keys: artists, song_name and isrc.
    :return: bool - True if the function ran successfully
    """
    # Get 15 YouTube search results based on the raw strings
    search_terms = track["artists"] + ' ' + track["song_name"] + ' official'
    results_1 = YoutubeSearch(search_terms, max_results=15).to_dict()

    # Get 15 YouTube search results based on the raw artists string and the cleaned title string
    search_terms = ct(search_terms)
    results_2 = YoutubeSearch(search_terms, max_results=15).to_dict()

    results = results_1 + results_2

    track["search_terms"] = search_terms
    track["cleaned_artist"] = cs(track["artists"])
    track["cleaned_song_name"] = cs(track["song_name"])

    # Loop over the search results
    for res_idx, result in enumerate(results):

        youtube_url = youtube_url_prefix + result['url_suffix']
        ydl_opts = {'quiet': True}

        # Extract metadata for the current results: meta is a long string with tons of info
        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
            meta = ydl.extract_info(
                youtube_url, download=False)

        # Try to get track duration from the metadata
        duration = 0
        try:
            duration = meta['duration']
        except Exception:
            logger.error("No duration in metadata.")

        # Add more data to the meta string from the search result
        meta = str(meta) + " "
        try:
            if "title" in result:
                meta += result["title"] + " "
        except Exception:
            logger.error("Cannot add title to metadata.")

        try:
            if "long_desc" in result:
                meta += result["long_desc"] + " "
        except Exception:
            logger.error("Cannot add long_desc to metadata.")

        try:
            if "channel" in result:
                meta += result["channel"]
        except Exception:
            logger.error("Cannot add channel to metadata.")

        # Clean the meta string removing accents, special characters, etc.
        meta = cs(meta)

        # Compute the token distance between the artists and the meta string (distance will be small if the artists
        # can be found in meta)
        artist_dist, artist_meta = get_asymmetric_token_distance(str_short=cs(track["artists"]), str_long=meta)

        # Compute the token distance between the song_name and the meta string (distance will be small if the song_name
        # can be found in meta)
        song_name_dist, song_name_meta = get_asymmetric_token_distance(str_short=cs(track["song_name"]), str_long=meta)
        check_official = "official" in meta

        # Computes a score evaluating the confidence we have that the current result is a good match for the input track
        # If neither of artists and song_name were found in the meta string, the score is 0.
        # If both were found EXACTLY, the score is 1
        # If one of them was found EXACTLY the score is 0.5
        # All intermediate values are possible if only some words from the title were found in meta for example
        confidence_score = ((1 - song_name_dist) + (1 - artist_dist)) / 2

        # If the duration exceeds 15 minutes, ignore the current result
        if duration >= 15*60:
            confidence_score = 0

        # Store the current result if it is the first search result of if the confidence is better than previous ones
        if res_idx == 0 or confidence_score > track["youtube_url_confidence"]:
            track["metadata_artist"] = artist_meta
            track["artist_dist"] = artist_dist

            track["metadata_song_name"] = song_name_meta
            track["song_name_dist"] = song_name_dist

            track["youtube_url_confidence"] = confidence_score

            # Keep whether "official" is found in the meta string or not, for debugging and score tuning.
            if check_official:
                track["official"] = "TRUE"

            track["youtube_url"] = youtube_url
            track["result_idx"] = res_idx

        # If the confidence score is higher than 0.8, assume it is a good enough match and break the loop
        if track["youtube_url_confidence"] > 0.8:
            break

    track["youtube_url_confidence"] = round(track["youtube_url_confidence"], 2)
    return True


def perform_extraction(track, len_tracks):
    """
    Run extraction for one track and check progress updating global counters
    :param track: dict - dictionary of the track containing the three keys: artists, song_name and isrc.
    :param len_tracks: int - total number of tracks to be processed
    :return: None
    """
    global lock
    global count
    global total
    global succeeded
    global failed

    total = len_tracks

    try:
        success = extract_youtube_link(track)
    except Exception as err:
        logger.error("Exception {}\nTraceback: {}".format(err, traceback.format_exc()))
        success = False

    lock.acquire()
    count += 1
    if success:
        succeeded += 1
    else:
        failed += 1

    if count % 2 == 0:
        status = round(100 * count / total, 2)
        logger.info("Count: " + str(count) + "/" + str(total) + " - " + str(status) + "%")
        logger.info("Succeeded: " + str(succeeded) + " - Failed = " + str(failed) + ".")
    lock.release()
