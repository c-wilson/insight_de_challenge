from edgar_sessionizer import sessionization, sources, sinks
import os


def csv_to_txt(log_directory, save_path):
    """

    :param log_directory:
    :param save_path:
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
    # input_dir = '/Users/chris/PycharmProjects/insight_de_challenge/insight_testsuite/tests/test_1/input'
    # save_path = '/Users/chris/PycharmProjects/insight_de_challenge/insight_testsuite/tests/test_1/output/run1.txt'
    input_dir = '/Users/chris/PycharmProjects/insight_de_challenge/playing'
    save_path = '/Users/chris/PycharmProjects/insight_de_challenge/playing/result1.txt'
    csv_to_txt(input_dir, save_path)