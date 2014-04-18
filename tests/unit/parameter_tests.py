import unittest
import pika


class ConnectionTests(unittest.TestCase):

    def test_parameters_accepts_plain_string_virtualhost(self):
        parameters = pika.ConnectionParameters(virtual_host="prtfqpeo")
        self.assertEqual(parameters.virtual_host, "prtfqpeo")

    def test_parameters_accepts_plain_string_virtualhost(self):
        parameters = pika.ConnectionParameters(virtual_host=u"prtfqpeo")
        self.assertEqual(parameters.virtual_host, "prtfqpeo")

    def test_parameters_accept_plain_string_locale(self):
        parameters = pika.ConnectionParameters(locale="en_US")
        self.assertEqual(parameters.locale, "en_US")

    def test_parameters_accept_unicode_locale(self):
        parameters = pika.ConnectionParameters(locale=u"en_US")
        self.assertEqual(parameters.locale, "en_US")

    def test_urlparameters_accepts_plain_string(self):
        parameters = pika.URLParameters("amqp://prtfqpeo:oihdglkhcp0@myserver.mycompany.com:5672/prtfqpeo?locale=en_US")
        self.assertEqual(parameters.port, 5672)
        self.assertEqual(parameters.virtual_host, "prtfqpeo")
        self.assertEqual(parameters.credentials.password, "oihdglkhcp0")
        self.assertEqual(parameters.credentials.username, "prtfqpeo")
        self.assertEqual(parameters.locale, "en_US")

    def test_urlparameters_accepts_unicode_string(self):
        parameters = pika.URLParameters(u"amqp://prtfqpeo:oihdglkhcp0@myserver.mycompany.com:5672/prtfqpeo?locale=en_US")
        self.assertEqual(parameters.port, 5672)
        self.assertEqual(parameters.virtual_host, "prtfqpeo")
        self.assertEqual(parameters.credentials.password, "oihdglkhcp0")
        self.assertEqual(parameters.credentials.username, "prtfqpeo")
        self.assertEqual(parameters.locale, "en_US")