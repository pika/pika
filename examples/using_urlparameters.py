from urllib.parse import urlencode


urlencode({
    'ssl_options': {
        'certfile': '/etc/ssl/mycert.pem',
        'keyfile': '/etc/ssl/mykey.pem',
    }
})
