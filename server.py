import configparser
import json
import os

from bottle import route, run, request, response, hook
from gdal_interfaces import GDALTileInterface


class InternalException(ValueError):
    """
    Utility exception class to handle errors internally and return error codes to the client
    """
    pass


print('Reading config file ...')
parser = configparser.ConfigParser()
parser.read('config.ini')

HOST = parser.get('server', 'host')
PORT = parser.getint('server', 'port')
NUM_WORKERS = parser.getint('server', 'workers')
DATA_FOLDER_30 = parser.get('server', 'data-folder_30')
DATA_FOLDER_120 = parser.get('server', 'data-folder_120')
DATA_FOLDER_240 = parser.get('server', 'data-folder_240')
OPEN_INTERFACES_SIZE = parser.getint('server', 'open-interfaces-size')
URL_ENDPOINT_30 = parser.get('server', 'endpoint_30')
URL_ENDPOINT_120 = parser.get('server', 'endpoint_120')
URL_ENDPOINT_240 = parser.get('server', 'endpoint_240')
URL_ENDPOINT = parser.get('server', 'endpoint')
ALWAYS_REBUILD_SUMMARY = parser.getboolean('server', 'always-rebuild-summary')
CERTS_FOLDER = parser.get('server', 'certs-folder')
# always set false to keep system safe currently
# CACHE_ALL_TIFF_FILES = parser.getboolean('server', 'cache_all_tiff_files')
CACHE_ALL_TIFF_FILES = False
CERT_FILE = '%s/cert.crt' % CERTS_FOLDER
KEY_FILE = '%s/cert.key' % CERTS_FOLDER


def build_summary(gdal_interface):
    if gdal_interface.has_summary_json() and not ALWAYS_REBUILD_SUMMARY:
        print('Re-using existing summary JSON')
        gdal_interface.read_summary_json()
    else:
        print('Creating summary JSON ...')
        gdal_interface.create_summary_json()


"""
Initialize a global interface. This can grow quite large, because it has a cache.
"""
interface_30 = GDALTileInterface(DATA_FOLDER_30, '%s/summary.json' % DATA_FOLDER_30, OPEN_INTERFACES_SIZE, CACHE_ALL_TIFF_FILES)
build_summary(interface_30)

interface_120 = GDALTileInterface(DATA_FOLDER_120, '%s/summary.json' % DATA_FOLDER_120, OPEN_INTERFACES_SIZE, CACHE_ALL_TIFF_FILES)
build_summary(interface_120)


interface_240 = GDALTileInterface(DATA_FOLDER_240, '%s/summary.json' % DATA_FOLDER_240, OPEN_INTERFACES_SIZE, CACHE_ALL_TIFF_FILES)
build_summary(interface_240)

def get_elevation(lat, lng, gdal_interface):
    """
    Get the elevation at point (lat,lng) using the currently opened interface
    :param lat:
    :param lng:
    :return:
    """
    try:
        elevation = gdal_interface.lookup(lat, lng)
    except:
        return {
            'latitude': lat,
            'longitude': lng,
            'error': 'No such coordinate (%s, %s)' % (lat, lng)
        }

    return {
        'latitude': lat,
        'longitude': lng,
        'elevation': elevation
    }


@hook('after_request')
def enable_cors():
    """
    Enable CORS support.
    :return:
    """
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'PUT, GET, POST, DELETE, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Origin, Accept, Content-Type, X-Requested-With, X-CSRF-Token'


def lat_lng_from_location(location_with_comma):
    """
    Parse the latitude and longitude of a location in the format "xx.xxx,yy.yyy" (which we accept as a query string)
    :param location_with_comma:
    :return:
    """
    try:
        lat, lng = [float(i) for i in location_with_comma.split(',')]
        return lat, lng
    except:
        raise InternalException(json.dumps({'error': 'Bad parameter format "%s".' % location_with_comma}))


def query_to_locations():
    """
    Grab a list of locations from the query and turn them into [(lat,lng),(lat,lng),...]
    :return:
    """
    locations = request.query.locations
    if not locations:
        raise InternalException(json.dumps({'error': '"Locations" is required.'}))

    return [lat_lng_from_location(l) for l in locations.split(';')]


def body_to_locations():
    """
    Grab a list of locations from the body and turn them into [(lat,lng),(lat,lng),...]
    :return:
    """
    try:
        locations = request.json.get('locations', None)
    except Exception:
        raise InternalException(json.dumps({'error': 'Invalid JSON.'}))

    if not locations:
        raise InternalException(json.dumps({'error': '"Locations" is required in the body.'}))

    latlng = []
    for l in locations:
        try:
            latlng += [(l['latitude'], l['longitude'])]
        except KeyError:
            raise InternalException(json.dumps({'error': '"%s" is not in a valid format.' % l}))

    return latlng


def do_lookup(get_locations_func, gdal_interface):
    """
    Generic method which gets the locations in [(lat,lng),(lat,lng),...] format by calling get_locations_func
    and returns an answer ready to go to the client.
    :return:
    """
    try:
        locations = get_locations_func()
        return {'results': [get_elevation(lat, lng, gdal_interface) for (lat, lng) in locations]}
    except InternalException as e:
        response.status = 400
        response.content_type = 'application/json'
        return e.args[0]


# For CORS
@route(URL_ENDPOINT_30, method=['OPTIONS'])
def cors_handler():
    return {}


@route(URL_ENDPOINT_30, method=['GET'])
def get_lookup():
    """
    GET method. Uses query_to_locations.
    :return:
    """
    return do_lookup(query_to_locations, interface_30)


@route(URL_ENDPOINT_30, method=['POST'])
def post_lookup():
    """
    GET method. Uses body_to_locations.
    :return:
    """
    return do_lookup(body_to_locations, interface_30)


# For CORS
@route(URL_ENDPOINT_120, method=['OPTIONS'])
def cors_handler():
    return {}


@route(URL_ENDPOINT_120, method=['GET'])
def get_lookup():
    """
    GET method. Uses query_to_locations.
    :return:
    """
    return do_lookup(query_to_locations, interface_120)


@route(URL_ENDPOINT_120, method=['POST'])
def post_lookup():
    """
    GET method. Uses body_to_locations.
    :return:
    """
    return do_lookup(body_to_locations, interface_120)


# For CORS
@route(URL_ENDPOINT_240, method=['OPTIONS'])
@route(URL_ENDPOINT, method=['OPTIONS'])
def cors_handler():
    return {}


@route(URL_ENDPOINT_240, method=['GET'])
@route(URL_ENDPOINT, method=['GET'])
def get_lookup():
    """
    GET method. Uses query_to_locations.
    :return:
    """
    return do_lookup(query_to_locations, interface_240)


@route(URL_ENDPOINT_240, method=['POST'])
@route(URL_ENDPOINT, method=['POST'])
def post_lookup():
    """
    GET method. Uses body_to_locations.
    :return:
    """
    return do_lookup(body_to_locations, interface_240)


if os.path.isfile(CERT_FILE) and os.path.isfile(KEY_FILE):
    print('Using HTTPS')
    run(host=HOST, port=PORT, server='gunicorn', workers=NUM_WORKERS, certfile=CERT_FILE, keyfile=KEY_FILE)
else:
    print('Using HTTP')
    run(host=HOST, port=PORT, server='gunicorn', workers=NUM_WORKERS)
