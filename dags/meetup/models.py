from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import MetaData, Table, Column, Integer, String, Date, Boolean,  DateTime, Float, ForeignKey

Base = declarative_base(metadata=MetaData(schema='source'))

topics_association_table = Table('topic__group', Base.metadata,
        Column('topic_id', Integer, ForeignKey('topic.id')),
        Column('group_id', Integer, ForeignKey('group.id')),
        schema='source'
)
venues_association_table = Table('venue__event', Base.metadata,
        Column('venue_id', Integer, ForeignKey('venue.id')),
        Column('event_id', String, ForeignKey('event.id')),
        schema='source'
)

class Group(Base):
    __tablename__ = 'group'

    id = Column(Integer, primary_key=True)
    category = relationship("Category", back_populates="group")
    category_id = Column(Integer, ForeignKey('category.id'))
    events = relationship("Event", back_populates="group")
    topics = relationship('Topic', secondary=topics_association_table, back_populates='group')
    created = Column(DateTime)
    urlname = Column(String)
    name = Column(String)
    description = Column(String)
    status = Column(String)
    join_mode = Column(String)
    city = Column(String)
    state = Column(String)
    lat = Column(Float)
    lon = Column(Float)
    country = Column(String)
    link = Column(String)
    members = Column(Integer)
    membership_dues = Column(String)
    who = Column(String)
    visibility = Column(String)
    upload_timestamp = Column(DateTime)

    def __repr__(self):
        return "<Group(name='%s')>" % (self.name)


class Event(Base):
    __tablename__ = 'event'

    id = Column(String, primary_key=True)
    group_id = Column(Integer, ForeignKey('group.id'))
    group = relationship("Group", back_populates="events")
    venue = relationship('Venue',
                         secondary=venues_association_table,
                         back_populates='events')
    created = Column(DateTime)
    name = Column(String)
    description = Column(String)
    status = Column(String)
    local_datetime = Column(DateTime)
    duration = Column(Float)
    rsvp_limit = Column(Integer)
    fee_amount = Column(Float)
    fee_currency = Column(String)
    fee_required = Column(Boolean)
    why = Column(String)
    visibility = Column(String)
    link = Column(String)
    waitlist_count = Column(Integer)
    yes_rsvp_count = Column(Integer)
    attendance_count = Column(Integer)
    manual_attendance_count = Column(Integer)
    comment_count = Column(Integer)
    upload_timestamp = Column(DateTime)

    def __repr__(self):
        return "<Event(name='%s')>" % (self.name)

class Category(Base):
    __tablename__ = 'category'

    id = Column(Integer, primary_key=True)
    group = relationship("Group", back_populates="category")
    name = Column(String)
    shortname = Column(String)
    sort_name = Column(String)
    upload_timestamp = Column(DateTime)

    def __repr__(self):
        return "<Category(name='%s')>" % (self.name)

class Topic(Base):
    __tablename__ = 'topic'

    id = Column(Integer, primary_key=True)
    group = relationship('Group',
                         secondary=topics_association_table,
                         back_populates='topics')
    name = Column(String)
    urlkey = Column(String)
    upload_timestamp = Column(DateTime)

    def __repr__(self):
        return "<Topic(name='%s')>" % (self.name)

class Venue(Base):
    __tablename__ = 'venue'

    id = Column(Integer, primary_key=True)
    events = relationship('Event',
                          secondary=venues_association_table,
                          back_populates='venue')
    name = Column(String)
    lat = Column(Float)
    lon = Column(Float)
    address_1 = Column(String)
    city = Column(String)
    state = Column(String)
    zip = Column(String)
    country = Column(String)
    upload_timestamp = Column(DateTime)

    def __repr__(self):
        return "<Venue(name='%s')>" % (self.name)

# Production Tables
BaseProd = declarative_base(metadata=MetaData(schema='prod'))

topics_bridge_table = Table('topic_group_bridge', BaseProd.metadata,
    Column('topic_key', Integer, ForeignKey('dim_topic.key')),
    Column('group_key', Integer, ForeignKey('dim_group.key')),
    schema='prod'
)

class DimEvent(BaseProd):
    __tablename__ = 'dim_event'

    key = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String)
    name = Column(String)
    description = Column(String)
    rsvp_limit = Column(Integer)
    fee_amount = Column(Float)
    fee_required = Column(Boolean)
    link = Column(String)

class DimTopic(BaseProd):
    __tablename__ = 'dim_topic'

    key = Column(Integer, primary_key=True, autoincrement=True)
    group = relationship('DimGroup',
                         secondary=topics_bridge_table,
                         back_populates='topics')
    topic_id = Column(Integer)
    name = Column(String)

class DimGroup(BaseProd):
    __tablename__ = 'dim_group'

    key = Column(Integer, primary_key=True, autoincrement=True)
    topics = relationship('DimTopic',
                          secondary=topics_bridge_table,
                          back_populates='group')
    group_id = Column(Integer)
    name = Column(String)
    description = Column(String)
    link = Column(String)

class DimVenue(BaseProd):
    __tablename__ = 'dim_venue'

    key = Column(Integer, primary_key=True, autoincrement=True)
    venue_id = Column(Integer)
    name = Column(String)
    lat = Column(Float)
    lon = Column(Float)
    city = Column(String)
    state = Column(String)
    zip = Column(String)
    country = Column(String)

class FactEvent(BaseProd):
    __tablename__ = 'fact_event'

    event_key = Column(Integer, primary_key=True)
    group_key = Column(Integer, primary_key=True)
    date_key = Column(Integer, primary_key=True)
    venue_key = Column(Integer)
    event_datetime = Column(DateTime)
    attendance_count = Column(Integer)
    yes_rsvp_count = Column(Integer)
    manual_attendance_count = Column(Integer)
    comment_count = Column(Integer)
    waitlist_count = Column(Integer)

class Transaction(BaseProd):
    __tablename__ = '_transactions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    loaded_until = Column(DateTime)
    status = Column(String)