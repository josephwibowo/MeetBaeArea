import datetime
import json
from meetup.models import Base, BaseProd, Group, Event, Category, Topic, Venue

# SQLalchemy funcs


def get_or_create(session, model, id_filter, **kwargs):
    instance = session.query(model).filter_by(id=id_filter).first()
    if instance:
        return instance
    else:
        instance = model(**kwargs)
        session.add(instance)
        session.commit()
        return instance

# Conversion Functions
def epoch_to_datetime(epoch):
    if epoch:
        return datetime.datetime.fromtimestamp(epoch/1000.0)
    else:
        return epoch

def str_to_datetime(local_date, local_time):
    datetime_str = local_date+' '+local_time
    return datetime.datetime.strptime(datetime_str, '%Y-%m-%d %H:%M')

def convert_to_json(dict):
    return json.dumps(dict)

# ETL scripts
def get_event_fee(fee):
    if fee:
        return fee
    else:
        return {'amount': None, 'currency': None, 'required': None}

def remove_venue_args(venue, upload_date):
    venue_args = {
        'id': venue['id'],
        'name': venue['name'],
        'lat': venue['lat'],
        'lon': venue['lon'],
        'address_1': venue['address_1'],
        'city': venue.get('city', None),
        'state': venue.get('state', None),
        'zip': venue.get('zip', None),
        'country': venue.get('country', None),
        'upload_timestamp': upload_date
     }
    return venue_args

def add_events_to_group(group_obj, events, db, upload_date):
    event_objs = associate_events(events, db, upload_date)
    for event in event_objs:
        group_obj.events.append(event)
    return group_obj

def associate_events(events, db, upload_date):
    # events: List of event objects. Each event object has max 200 events in event.results.
    event_list = []
    for event_obj in events:
        for event in event_obj.results:
            fee = get_event_fee(event.fee)
            event_raw = Event(id=event.id,
                     created=epoch_to_datetime(event.created),
                     name=event.name,
                     description=event.plain_text_description,
                     status=event.status,
                     local_datetime=str_to_datetime(event.local_date, event.local_time),
                     duration=event.duration,
                     rsvp_limit=event.rsvp_limit,
                     fee_amount=fee['amount'],
                     fee_currency=fee['currency'],
                     fee_required=fee['required'],
                     why=event.why,
                     visibility=event.visibility,
                     link=event.short_link,
                     waitlist_count=event.waitlist_count,
                     yes_rsvp_count=event.yes_rsvp_count,
                     attendance_count=event.attendance_count,
                     manual_attendance_count=event.manual_attendance_count,
                     comment_count=event.comment_count,
                     upload_timestamp=upload_date
                     )
            if event.venue:
                venue_kwargs = remove_venue_args(event.venue, upload_date)
                venue = get_or_create(db, Venue, venue_kwargs['id'], **venue_kwargs)
                event_raw.venue.append(venue)
            event_list.append(event_raw)
    return event_list

def associate_topics(db, topics, upload_date):
    topic_list = []
    for topic in topics:
        topic_kwargs = {'id': topic['id'],
                        'name': topic['name'],
                        'urlkey': topic['urlkey'],
                        'upload_timestamp': upload_date}
        t = get_or_create(db, Topic, topic['id'], **topic_kwargs)
        topic_list.append(t)
    return topic_list

def load_groups(objects, db, upload_date):
    for object in objects:
        for group in object.results:
            group_obj = db.query(Group).filter_by(id=group.id).first()
            # group has no events
            if group.events_count == 0:
                continue
            # group exists and new events were added
            elif group_obj and group.events_count > 0:
                add_events_to_group(group_obj, group.events, db, upload_date)
            # group does not exist
            else:
                events = associate_events(group.events, db, upload_date)
                topics = associate_topics(db, group.topics, upload_date)
                group.category['upload_timestamp'] = upload_date
                category = get_or_create(db, Category, group.category['id'], **group.category)
                raw_group = Group(id=group.id,
                                  events=events,
                                  topics=topics,
                                  created=epoch_to_datetime(group.created),
                                  urlname=group.urlname,
                                  name=group.name,
                                  description=group.plain_text_description,
                                  status=group.status,
                                  join_mode=group.join_mode,
                                  category=category,
                                  city=group.city,
                                  state=group.state,
                                  lat=group.lat,
                                  lon=group.lon,
                                  country=group.country,
                                  link=group.link,
                                  members=group.members,
                                  membership_dues=convert_to_json(group.membership_dues), #json
                                  who=group.who,
                                  visibility=group.visibility,
                                  upload_timestamp=upload_date
                                 )
                db.add(raw_group)
    db.commit()
    return


#_____MAIN_______
# 1) Extract and Load from Meetup API
# load_groups(all_groups)
#
# 2) Transform to STAR Schema
     # Execute a bunch of SQL queries
     # Stage only data you DO NOT have, AKA) only stage events from yesterday
     # For dims: Add upload timestamp, stage only data uploaded today
# 3) Replication method