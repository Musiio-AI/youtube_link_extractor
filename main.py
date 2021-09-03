from constants import YOUTUBE_LINK_EXTRACTOR_LOGGER, INPUT_DIR, OUTPUT_DIR, OUTPUT_FILE_PATH, LOG_DIR
from config import RESUME
from data_helpers import build_dataframe, write_dataframe
import logging
from functools import partial
from multiprocessing.pool import ThreadPool
from extraction_helpers import perform_extraction
import time
import os

# Initialize logger
timestr = time.strftime("%Y%m%d-%H%M%S")
logger = logging.getLogger(YOUTUBE_LINK_EXTRACTOR_LOGGER)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.FileHandler(str(LOG_DIR) + "/" + timestr + '-youtube_link_extractor.log'))
logger.addHandler(logging.StreamHandler())


if __name__ == '__main__':
    # Create output and logging directories
    if not os.path.isdir(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    if not os.path.isdir(LOG_DIR):
        os.makedirs(LOG_DIR)

    logger.info("Starting YouTube Links Extractor.")
    # Load data
    tracks = build_dataframe(input_csv_dir=INPUT_DIR,
                             resume=RESUME,
                             output_csv_path=OUTPUT_FILE_PATH,
                             logger_name=YOUTUBE_LINK_EXTRACTOR_LOGGER,
                             subset=None)

    # Capture program start time
    start_time = time.perf_counter()

    if len(tracks) > 0:
        # Run extraction
        with ThreadPool(10) as pool:
            logger.info("Processing: " + str(len(tracks)) + " tracks.")
            _ = pool.map(partial(perform_extraction, len_tracks=len(tracks)), tracks)

        write_dataframe(list_of_dict=tracks,
                        output_csv_path=OUTPUT_FILE_PATH,
                        resume=RESUME,
                        logger_name=YOUTUBE_LINK_EXTRACTOR_LOGGER)

    # Capture program execution time
    end_time = time.perf_counter()
    execution_time = (end_time - start_time)
    logger.info("Processed {} tracks in {} seconds ({} sec per track).".format(len(tracks),
                                                                               execution_time,
                                                                               execution_time/len(tracks)))
