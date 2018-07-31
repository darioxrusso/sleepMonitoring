#!/usr/bin/python

## A server application that collects data from a source file and sends them
#  to a client application to be elaborated.

import csv
import socket
import sys
import time

## Manages the source of the data.
class CsvSourceData:
    """Manages the source of the data."""

    ## Extracts data from a csv file with a specific format and release them
    # as a Python generator.
    #  @param csvFile The file containing data.
    def dataGenerator(self, csvFile):
        data = csv.reader(open(csvFile, 'rt'))
        for row in data:
            yield row[1] + " " + row[2] + " " + row[3] + " " + row[4] + " " + \
                  row[5] + " " + row[6]

## The server socket that sends data extracted from CsvSourceData class.
class ServerSocket:
    """The server socket that sends data extracted from CsvSourceData class."""

    ## Initialize the socket.
    #  @param server_name The name of the host where the server should be
    #                     created.
    #  @param server_port The port number where the server should be created.
    def __init__(self, server_name, server_port):
        # Create a TCP/IP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Bind the socket to the address given on the command line
        server_address = (server_name, int(server_port))
        self.sock.bind(server_address)

    ## Starts the server.
    #  @param dataGererator The python generator of the data.
    def start(self, dataGenerator):
        print >> sys.stderr, "Starting up on %s port %s" % self.sock.getsockname()
        self.sock.listen(1)
        print >> sys.stderr, "Waiting for a connection"
        connection, client_address = self.sock.accept()
        while True:
            try:
                print >> sys.stderr, "Client connected:", client_address
                while True:
                    connection.sendall(next(dataGenerator))
                    time.sleep(1.5)
            finally:
                connection.close()

## The main function.
# @param argv CLI parameters.
def main(argv):
    source_data = CsvSourceData()
    server = ServerSocket(argv[0], argv[1])
    data = source_data.dataGenerator(
        '/home/dario/workspace/isti-ha/projects/activage/eclipse-workspace/'
        + 'sonno/python_on_localhost/source_data/'
        + 'nl_14_fino_0420_ordered.csv')
    server.start(data)

if __name__ == "__main__":
    main(sys.argv[1:])
