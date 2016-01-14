"""
Zookeeper plugin polls Zookeeper for stats

"""
import logging
import re

from newrelic_plugin_agent.plugins import base

LOGGER = logging.getLogger(__name__)


class Zookeeper(base.SocketStatsPlugin):

    GUID = 'com.build.newrelic_zookeeper_agent'
    DEFAULT_PORT = 2181

    def add_datapoints(self, stats):
        """Add all of the data points for a node

        :param dict stats: all of the stats for a node

        """
        self.add_gauge_value('Connections',
                             'connections',
                             stats.get('connections', 0))
        self.add_gauge_value('Client Count',
                             'clients',
                             len(stats.get('clients')))
        self.add_gauge_value('Latency',
                             'seconds',
                             float(stats.get('latency')) / 1000)
        self.add_gauge_value('Node Count',
                             'nodes',
                             stats.get('node_count'), 0)
        self.add_derive_value('Received',
                              'bytes',
                              stats.get('received'), 0)
        self.add_derive_value('Sent',
                              'bytes',
                              stats.get('sent'), 0)
        for client_stats in stats.get('clients'):
            # Do not include the client if it is this agent
            if client_stats.get('received') != 1 or client_stats.get('sent') != 0:
                self.add_client_datapoints(client_stats)

    def add_client_datapoints(self, client_stats):
        """Add all of the data points for a client

        :param dict client_stats: all of the stats for a node's client

        """
        base_name = 'Clients/%s:%s' % (client_stats.get('host'), client_stats.get('port'))
        print base_name
        self.add_gauge_value('%s/Queued' % base_name,
                             'queued',
                             client_stats.get('queued', 0))
        self.add_derive_value('%s/Received' % base_name,
                              'bytes',
                              client_stats.get('received', 0))
        self.add_derive_value('%s/Sent' % base_name,
                              'bytes',
                              client_stats.get('sent', 0))

    def fetch_data(self, connection):
        """Loop in and read in all the data until we have received it all.

        :param  socket connection: The connection
        :rtype: dict

        """
        connection.send("stat\n")
        data = super(Zookeeper, self).fetch_data(connection)
        data_in = []
        for line in data.replace('\r', '').split('\n'):
            data_in.append(line.strip())
        return self.process_data(data_in)

    def process_data(self, data):
        """Loop through all the rows and parse each line into key value pairs.
        Special handling is done for the 'Clients' array and the multivalued key
        latency.

        :param list data: The list of rows
        :returns: dict

        """
        values = dict()
        cnt = len(data)
        index = 0
        client_pattern = re.compile(
            '^/([^: ]+):([0-9]+)\[([0-9+])\]\(queued=([0-9]+),recved=([0-9]+),sent=([0-9]+)')
        while index < cnt:
            row = data[index]
            parts = row.split(':')
            if parts[0] == 'Clients':
                # Read in clients until a blank line is found
                clients = []
                getting_clients = True
                while getting_clients:
                    index += 1
                    row = data[index]
                    if row == '':
                        getting_clients = False
                    else:
                        client_parts = client_pattern.match(row)
                        if client_parts:
                            clients.append({
                                'host': client_parts.group(1),
                                'port': client_parts.group(2),
                                'queued': int(client_parts.group(4)),
                                'received': int(client_parts.group(5)),
                                'sent': int(client_parts.group(6))})
                values['clients'] = clients
            elif parts[0] == 'Latency min/avg/max':
                parts = parts[1].split('/')
                if len(parts) == 3:
                    values['latency'] = int(parts[1])
            elif parts[0]:
                if parts[1].strip().isdigit():
                    values[parts[0].lower().replace(' ', '_')] = int(parts[1].strip())
            index += 1
        return values
