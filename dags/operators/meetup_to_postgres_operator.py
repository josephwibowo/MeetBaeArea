import time
from airflow.models import BaseOperator
from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker, scoped_session

from meetup.meetup import Meetup
from meetup.constants import EVENT_KWARGS, GROUP_KWARGS
from operators.etl import load_groups

class MeetupToPostgresOperator(BaseOperator):
    template_fields = ('start', 'end')

    def __init__(self,
                 engine,
                 start,
                 end,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.engine = engine
        self.start = start
        self.end = end

    @contextmanager
    def db_session(self):
        """ Creates a context with an open SQLAlchemy session.
        """
        connection = self.engine.connect()
        db_session = scoped_session(sessionmaker(autocommit=False, autoflush=True, bind=self.engine))
        yield db_session
        db_session.close()
        connection.close()

    def execute(self, context):
        m = Meetup()
        # Get all Groups
        all_groups = m.get_groups(**GROUP_KWARGS)
        with self.db_session() as db:
            for group_obj in all_groups:
                for group in group_obj.results:
                    time.sleep(0.5)
                    EVENT_KWARGS['no_earlier_than'] = self.start
                    EVENT_KWARGS['no_later_than'] = self.end
                    group.get_events(m, **EVENT_KWARGS)
            load_groups(all_groups, db, self.start)