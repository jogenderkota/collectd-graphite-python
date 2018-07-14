import argparse, os, sys, requests
import logging

logging.basicConfig(filename ='graphite-logs.conf', level=logging.DEBUG)

# Creating a parser object
parser = argparse.ArgumentParser(description="Process user input to archive the metric-data")

# Use Graphite API to extract the metric-data.
# Eg: &target=carbon.agents.graphite1.cpuUsage&from=-5min&rawData=true
graphite_api_url = "http://graphite_server_ip/render?target={mg}&format=json&from={start}&until={end}"

dest_path = os.getenv("HOME")+'/archives/'

def archive_data(datapoints, args):
    """This function Archives to file in a group-metric dir.
    Accepts list of datapoints with timestamps
    returns the archive's file path
    """
    metric_group_dir_path = dest_path + args.metric_group

    try:
        os.mkdir(metric_group_dir_path, 0755)
    except OSError as error:
        print("Couldn't create directory to archive data: {0}".format(error))
        raise

    # Handle granularity/frequency of data points
    freq = datapoints[1][1] - datapoints[0][1]
    start_timestamp = datapoints[0][1]

    # Create dictionary of datapoint values
    dp_dict = dict(datapoints)

    file_path = os.path.join(metric_group_dir_path, "{start}@{freq}".format(
        start=start_timestamp, freq=freq))

    # Save metric-data as archives
    logging.debug("Saving in file: {0}".format(file_path))
    with open(file_path, 'a') as tmpfile:
        try:
            tmpfile.write("\n")
            tmpfile.write(str(dp_dict))
        except IOError:
            raise

    return file_path


def extract_metric_data(args):
    """Extract data from graphite db and return metric datapoints in json format
    """
    metric_data = requests.get(graphite_api_url.format(mg=args.metric_group, start=args.start, end=args.end))
    #print metric_data.text()
    #print metric_data.status_code()
    #print metric_data.json()
    return metric_data.json()


def main():
    """This is the Main function
    """
    # Create a parser object
    parser = argparse.ArgumentParser(description="Process user input to archive the metric-data")

    parser.add_argument('--metric_group', help='the metric-group-name')
    parser.add_argument('--start', type=int, help="the start timestamp")
    parser.add_argument('--end', type=int, help="the end timestamp")
    # Parse user entered input
    args = parser.parse_args()
    metric_data = extract_metric_data(args)

    # Archive the datapoints from json-data and handle failure.
    try:
        file_path = archive_data(metric_data[0]['datapoints'], args)
    except OSError as err:
        logging.error("Failed to archive metric-data")
        print("Failed to archive metric-data: {0}".format(err))
        raise


if __name__ == '__main__':
    main()
