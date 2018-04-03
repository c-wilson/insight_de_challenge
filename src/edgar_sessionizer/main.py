"""
Entry-point scripts that make instances of DataSource, Sessionizer and Sink classes.
"""

from edgar_sessionizer import sessionization, sources, sinks
import os


def csv_to_txt(log_directory, save_path):
    """
    Runs Sessionizer with CSV input source and output source as per coding challenge spec.

    :param log_directory: Directory to the input directory containing "log.csv" and "inactivity_period.txt" files
    :param save_path: Path to save output.
    :return:
    """

    # load inactivity.
    inact_path = os.path.join(log_directory, 'inactivity_period.txt')
    with open(inact_path, 'r') as f:
        inact_str = f.read()
    inact_period = float(inact_str)

    # open log csv file
    log_path = os.path.join(log_directory, 'log.csv')

    # open sink that saves the data file object
    with sources.CsvSource(log_path) as source, sinks.CsvSink(save_path) as sink:
        processor = sessionization.Sessionizer(source, sink, inact_period)
        processor.run()


if __name__ == '__main__':
    # default inputs and outputs based on challenge spec:
    input_dir = './input'
    save_path = './output/sessionization.txt'

    csv_to_txt(input_dir, save_path)
