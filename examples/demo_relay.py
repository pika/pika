#!/usr/bin/env python

import pika
import asyncore

class Relay:
    def __init__(self,
                 source_param,
                 target_params,
                 exchange_name,
                 exchange_type = "fanout",
                 exchange_durable = True,
                 bind_routing_key = "",
                 relay_durable = True):
        self.relay_name = source_param[0]
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.exchange_durable = exchange_durable
        self.bind_routing_key = bind_routing_key
        self.relay_durable = relay_durable

        self.source_conn = self.fresh_connection(source_param[1])

        self.target_conns = dict((id, self.fresh_connection(p)) for (id, p) in target_params)
        self.target_conn_ids = dict((c, id) for (id, c) in self.target_conns.iteritems())
        self.target_chs = {}

        self.reset_pending_lists()

        self.source_conn.addStateChangeHandler(self.handle_source_connection_state_change)
        for c in self.target_conns.itervalues():
            c.addStateChangeHandler(self.handle_target_connection_state_change)

    def fresh_connection(self, parameters):
        strategy = pika.SimpleReconnectionStrategy()
        return pika.AsyncoreConnection(parameters,
                                       wait_for_open = False,
                                       reconnection_strategy = strategy)

    def reset_pending_lists(self):
        self.pending_deliveries = dict((c, []) for c in self.target_conns.itervalues())
        self.pending_acks = {}

    def handle_channel_state_change(self, ch, is_open):
        print 'Channel state change', ch.channel_number, ch.connection.parameters, ch.channel_close, is_open
        if not is_open:
            ch.connection.ensure_closed()

    def handle_target_connection_state_change(self, conn, is_connected):
        print 'Target state change', conn.parameters, is_connected
        if is_connected:
            ch = conn.channel()
            self.target_chs[conn] = ch
            ch.addStateChangeHandler(self.handle_channel_state_change)
            ch.exchange_declare(exchange = self.exchange_name,
                                type = self.exchange_type,
                                durable = self.exchange_durable)
            retry_list = self.pending_deliveries[conn]
            self.pending_deliveries[conn] = []
            print 'Target connected; %d pending deliveries;' % (len(retry_list)), conn.parameters

            for delivery in retry_list:
                if self.attempt_delivery(conn, delivery):
                    dt = delivery[0].delivery_tag
                    c = self.pending_acks[dt] - 1
                    self.pending_acks[dt] = c
                    print 'Remaining acks for dt %d: %d' % (dt, c)
                    if c == 0:
                        self.source_ch.basic_ack(delivery_tag = dt)
                        del self.pending_acks[dt]
        else:
            self.target_chs.pop(conn, None)
            print 'Target disconnected', conn.parameters

    def handle_source_connection_state_change(self, conn, is_connected):
        print 'Source state change', conn.parameters, is_connected
        self.reset_pending_lists()

        if is_connected:
            self.source_ch = conn.channel()
            self.source_ch.addStateChangeHandler(self.handle_channel_state_change)

            self.source_ch.exchange_declare(exchange = self.exchange_name,
                                            type = self.exchange_type,
                                            durable = self.exchange_durable)

            if self.relay_durable:
                declare_ok = \
                    self.source_ch.queue_declare(queue = self.relay_name,
                                                 durable = True,
                                                 auto_delete = False)
            else:
                declare_ok = \
                    self.source_ch.queue_declare(queue = "",
                                                 durable = False,
                                                 auto_delete = True)

            self.queue_name = declare_ok.queue

            self.source_ch.queue_bind(queue = self.queue_name,
                                      exchange = self.exchange_name,
                                      routing_key = self.bind_routing_key)

            print 'Source connected to queue %s; %d messages waiting' % (self.queue_name,
                                                                         declare_ok.message_count)
            self.source_tag = self.source_ch.basic_consume(self.handle_delivery,
                                                           queue = self.queue_name)
        else:
            self.source_ch = None
            self.queue_name = None
            self.source_tag = None
            print 'Source disconnected with reason', conn.connection_close

    def attempt_delivery(self, conn, delivery):
        (method, header, body) = delivery
        try:
            self.target_chs[conn].basic_publish(exchange = self.exchange_name,
                                                routing_key = method.routing_key,
                                                body = body,
                                                properties = header)
            return True
        except:
            self.pending_deliveries[conn].append(delivery)
            return False

    def handle_delivery(self, method, header, body):
        #print "method=%r" % (method,)
        #print "header=%r" % (header,)
        #print "  body=%r" % (body,)

        seenset = set((header.app_id or '').split())
        already_seen = self.relay_name in seenset

        # We implement here the multicast/completely-connected
        # strategy of only relaying messages that have been through no
        # relay at all. The alternative is to relay on to those
        # targets whose identifiers don't appear in the list, which
        # depending how you do it can provide a token-ring-style or
        # bittorrent-style system.
        should_relay = not seenset

        print "  seen=%r, already_seen=%r, should_relay=%r" % (seenset, already_seen, should_relay)

        seenset.add(self.relay_name)
        header.app_id = ' '.join(seenset)

        if not should_relay:
            self.source_ch.basic_ack(delivery_tag = method.delivery_tag)
        else:
            delivery = (method, header, body)
            retry_count = 0
            for c in self.target_conns.itervalues():
                if not self.attempt_delivery(c, delivery):
                    retry_count = retry_count + 1

            if retry_count:
                print 'Failed to deliver dt %d to %d targets' % (method.delivery_tag, retry_count)
                self.pending_acks[method.delivery_tag] = retry_count
            else:
                self.source_ch.basic_ack(delivery_tag = method.delivery_tag)

if __name__ == "__main__":
    import optparse
    parser = optparse.OptionParser()

    parser.add_option("-p", "--port", type="int",
                      help="set port number for subsequent server definitions")
    parser.add_option("-U", "--user",
                      help="set username for subsequent server definitions")
    parser.add_option("-P", "--password",
                      help="set password number for subsequent server definitions")
    parser.add_option("-v", "--virtual-host",
                      help="set virtual-host for subsequent server definitions")

    def spec_for(id_and_host, parser):
        try:
            (id, hostname) = id_and_host.split(":", 1)
        except:
            print \
                'Invalid identifier-and-hostname "%s": must be of the form "identifier:hostname"' %\
                (id_and_host,)
            parser.print_help()
            parser.exit()

        if parser.values.user:
            creds = pika.PlainCredentials(parser.values.user, parser.values.password)
        else:
            creds = None
        return (id, pika.ConnectionParameters(hostname,
                                              parser.values.port or None,
                                              parser.values.virtual_host or "/",
                                              creds))

    def set_source(option, opt_str, value, parser):
        setattr(parser.values, "source", spec_for(value, parser))

    def add_target(option, opt_str, value, parser):
        parser.values.ensure_value("targets", []).append(spec_for(value, parser))

    parser.add_option("-S", "--source", action="callback", callback=set_source, type="string",
                      help="(REQUIRED) set source server id and host")
    parser.add_option("-T", "--target", action="callback", callback=add_target, type="string",
                      dest="targets", help="(REQUIRED) add a target server host")
    parser.add_option("-x", "--exchange", help="(REQUIRED) set exchange name")
    parser.add_option("-t", "--exchange-type", default="fanout", help="set exchange type")
    parser.add_option("--durable-exchange", dest="durable_exchange", default=True,
                      action="store_true", help="use durable exchange")
    parser.add_option("--transient-exchange", dest="durable_exchange", action="store_false",
                      help="use transient exchange")
    parser.add_option("-k", "--binding-key", default="", help="set binding key")
    parser.add_option("--durable", dest="durable_relay", default=True,
                      action="store_true", help="use durable relay")
    parser.add_option("--transient", dest="durable_relay", action="store_false",
                      help="use transient relay")

    (options, args) = parser.parse_args()

    if not (options.source and options.targets and options.exchange):
        print 'Required argument missing'
        parser.print_help()
        parser.exit()

    relay = Relay(options.source,
                  options.targets,
                  options.exchange,
                  options.exchange_type,
                  options.durable_exchange,
                  options.binding_key,
                  options.durable_relay)

    pika.asyncore_loop()
