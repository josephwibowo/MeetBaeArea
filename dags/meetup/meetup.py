import datetime
import logging
import time
import json
import os
from urllib.parse import urlencode, quote_plus
from urllib.request import HTTPError, HTTPErrorProcessor, build_opener

logging.basicConfig(filename='logs/etl.log', format='%(asctime)s %(message)s')
logging.getLogger().setLevel(logging.DEBUG)
API_JSON_ENCODING = 'utf-8'

try:
    try:
        import cjson
        parse_json = lambda s: cjson.decode(s.decode(API_JSON_ENCODING), True)
    except ImportError:
        try:
            import json
            parse_json = lambda s: json.loads(s.decode(API_JSON_ENCODING))
        except ImportError:
            import simplejson
            parse_json = lambda s: simplejson.loads(s.decode(API_JSON_ENCODING))
except:
    print("Error - your system is missing support for a JSON parsing library.")

# Create a Requests Session Class with simple api key
API_BASE_URL = 'http://api.meetup.com/'

GROUPS_URI = 'find/groups'
EVENTS_URI = '{}/events'

READ_METHODS = {
        'groups': 'find/groups',
        'events': '{}/events'
    }

class MeetupHTTPErrorProcessor(HTTPErrorProcessor):
    def http_response(self, request, response):
        try:
            return HTTPErrorProcessor.http_response(self, request, response)
        except HTTPError as e:
            data = e.read()

            try:
                error_json = parse_json(data)
            except ValueError:
                logging.debug('Value error when trying to parse JSON from response data:\n%s' % response)
                raise

            if e.code == 401:
                raise UnauthorizedError(error_json)
            elif e.code in ( 400, 500 ):
                raise BadRequestError(error_json)
            else:
                raise ClientException(error_json)

class Meetup:
    opener = build_opener(MeetupHTTPErrorProcessor)
    def __init__(self):
        with open(os.getcwd() + '/dags/meetup/config.json') as f:
            data = json.load(f)
            self.key = data['MEETUP_KEY']
        self.params = {'sign': 'true', 'key': self.key}

    def args_str(self, url_args):
        if self.params:
            for k, v in self.params.items():
                url_args[k] = v
        return urlencode(url_args)

    def _fetch_groups(self, uri, paginate=False, **url_args):
        args = self.args_str(url_args)
        if not paginate:
            url = API_BASE_URL + uri + '/' + "?" + args
        else:
            url = uri + '&key=' + self.key

        response = self.opener.open(url)
        json_response = parse_json(response.read())
        headers = response.info()
        return json_response, headers

    def _fetch_events(self, urlname, uri, **url_args):
        args = self.args_str(url_args)
        url = API_BASE_URL + uri.format(quote_plus(urlname)) + '/' + "?" + args
        response = self.opener.open(url)
        json_response = parse_json(response.read())
        headers = response.info()
        return json_response, headers

    def get_groups(self, **args):
        # returns API_Response object which is a container for json response
        # Pass in the json using fetch to request the url
        responses = []
        json_r, headers = self._fetch_groups(GROUPS_URI, **args)
        responses.append(API_Response(json_r, headers, GROUPS_URI))

        # paginate
        if headers['Link']:
            link_rel = headers['Link'].split('rel=')[-1].replace('"', '')
            while link_rel=='next':
                time.sleep(0.5)
                link = headers['Link'].split('>')[0].replace('<', '')
                json_r, headers = self._fetch_groups(link, paginate=True)
                link_rel = headers['Link'].split('rel=')[-1].replace('"', '')
                responses.append(API_Response(json_r, headers, GROUPS_URI))
        return responses

    def get_events(self, urlname, **args):
        counter = 0
        responses = []
        events, headers = self._fetch_events(urlname, EVENTS_URI, **args)
        Events = API_Response(events, headers, EVENTS_URI)
        counter += len(Events.results)
        responses.append(Events)
        if Events.results:
            first_event_id = Events.results[0].id

        while len(Events.results) == 200:
            time.sleep(0.5)
            last_event = Events.results[-1]
            local_date = str(datetime.datetime.strptime(last_event.local_date, '%Y-%m-%d').date() + datetime.timedelta(days=1))
            iso_date = '{}T00:00:00.000'.format(local_date)
            args['no_earlier_than'] = iso_date
            events, headers = self._fetch_events(urlname, EVENTS_URI, **args)
            Events = API_Response(events, headers, EVENTS_URI)
            if first_event_id == Events.results[0].id:
                break
            else:
                first_event_id = Events.results[0].id
            counter += len(Events.results)
            responses.append(Events)
        return responses, counter

class API_Response(object):
    def __init__(self, json, headers, uritype):
        """Creates an object to act as container for API return val. Copies metadata from JSON"""
        self.meta = headers
        uriclasses = {GROUPS_URI: Group, EVENTS_URI: Event}
        self.results = [uriclasses[uritype](item) for item in json]


    def __str__(self):
        return 'meta: ' + str(self.meta) + '\n' + str(self.results)

class API_Item(object):
    """Base class for an item in a result set returned by the API."""

    datafields = [] #override
    def __init__(self, properties):
         """load properties that are relevant to all items (id, etc.)"""
         for field in self.datafields:
             # Not all fields are required to be returned
             if field in properties:
                self.__setattr__(field, properties[field])
             else:
                 self.__setattr__(field, None)
         self.json = properties

    def __repr__(self):
         return self.__str__();

class Group(API_Item):
    """Inherits the API_Item class which sets the object properties based off the datafields"""
    datafields = ['category', 'city', 'country', 'created', 'description', 'id', 'join_mode', 'lat', 'link', 'lon',
                  'plain_text_description', 'members', 'membership_dues', 'name', 'self', 'short_link', 'state',
                  'status', 'topics', 'urlname', 'visibility', 'who']

    def __str__(self):
         return "%s (%s)" % (self.name, self.link)

    def get_events(self, apiclient, **extraparams):
        events, event_count = apiclient.get_events(self.urlname, **extraparams)
        self.events = events
        self.events_count = event_count
        return

class Event(API_Item):
    """Inherits the API_Item class which sets the object properties based off the datafields"""
    datafields = ['attendance_count', 'comment_count', 'created', 'duration', 'fee', 'id', 'short_link', 'local_date',
                  'local_time', 'manual_attendance_count', 'name', 'past_event_count_inclusive',
                  'plain_text_description', 'rsvp_limit', 'status', 'venue', 'waitlist_count', 'why', 'yes_rsvp_count',
                  'visibility']

class ClientException(Exception):
    """
         Base class for generic errors returned by the server
    """
    def __init__(self, error_json):
        self.errors = error_json

    def __str__(self):
        return "%s" % (self.errors)

class UnauthorizedError(ClientException):
    pass;

class BadRequestError(ClientException):
    pass;


# DONE: Hit API for all Groups/Events raw data
    # Write script to validate group does not exist in database
        # Call get groups, then for each group, get or create
    # Write script to add new events since the most recent event
    # Write script to transform raw data and extract json data
        # category (DONE), topics (DONE)
        # fee (DONE), venue (DONE)
# TO DO:
    # Create a set group?
    # Create web app dashboard
