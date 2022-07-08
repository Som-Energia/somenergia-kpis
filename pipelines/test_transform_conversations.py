import unittest

from sqlalchemy_utils import (
    assert_nullable,
    assert_non_nullable,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, DateTime, String, ForeignKey, create_engine
from sqlalchemy.orm import relationship

import pendulum

from .hs_transform_conversations import hs_clean_conversation_from_dict

import sys
try:
    # The insertion index should be 1 because index 0 is this file
    sys.path.insert(1, './classes')  # the type of path is string #perque sempre
    # because the system path already have the absolute path to folder a
    # so it can recognize file_a.py while searching
    from models import (
        Base,
        HS_clean_conversation,
        HS_tag,
        Conversation_tag
    )
except (ModuleNotFoundError, ImportError) as e:
    print(f"{type(e)} failure")
else:
    print("Import succeeded")

# from classes.models import (
#     Base,
#     HS_clean_conversation,
#     HS_tag,
#     Conversation_tag
# )


class HelpscoutTransformTest(unittest.TestCase):

    engine = create_engine('sqlite:///:memory:')
    Session = sessionmaker(bind=engine)
    session = Session()

    def setUp(self):
        Base.metadata.create_all(self.engine)
        self.tag = HS_tag(id=1, name='test_tag')
        self.session.add(self.tag)
        self.conv = HS_clean_conversation(id=1, number=2, id_helpscout=3, customerWaitingSince_time=None, tags=[self.tag])
        self.session.add(self.conv)

        self.session.commit()

    def tearDown(self):
        Base.metadata.drop_all(self.engine)

    def base_hs_data(self):
        sample_time = '1970-01-01 00:00:00'
        data = {
            'number': 1000,
            'id': 1000,
            'threads': 1000,
            'type': 'lolo',
            'folderId': 1000,
            'status': 'blabla',
            'state': 'blabla',
            'subject': 'blabla',
            'mailboxId': 1,
            'createdAt': sample_time,
            'closedBy': 'blabla',
            'closedAt': sample_time,
            'userUpdatedAt': sample_time,
            'cc': '',
            'bcc': '',
            'createdBy': {
                'id': 1000,
                'email': 'blabla@example.com',
            },
            'closedByUser': {
                'email': 'blabla@example.com',
            },
            'customerWaitingSince': {
                'time': sample_time
            },
            'source': {
                'type': 'blabla',
                'via': 'blabla'
            },
            'primaryCustomer': {
                'id': 1000,
                'email': 'blabla@example.com'
            },
            'assignee': {
                'id': 1000,
                'email': ''
            },
            'tags': [{
                'id': 1000
            }]
        }

        dict_tags = {1000 : 'blabla'}
        return data, dict_tags

    def base_hs_conv(self):
        sample_time = '1970-01-01T00:00:00+00:00'
        return {
            'number': 1000,
            'id_helpscout': 1000,
            'threads': 1000,
            'type': 'lolo',
            'folderId': 1000,
            'status': 'blabla',
            'state': 'blabla',
            'subject': 'blabla',
            'mailboxId': 1,
            'createdAt': sample_time,
            'closedBy': 'blabla',
            'closedAt': sample_time,
            'userUpdatedAt': sample_time,
            'cc': '',
            'bcc': '',
            'createdBy_id': 1000,
            'createdBy_email': 'blabla@example.com',
            'closedByUser_email': 'blabla@example.com',
            'customerWaitingSince_time': sample_time,
            'source_type': 'blabla',
            'source_via': 'blabla',
            'primaryCustomer_id': 1000,
            'primaryCustomer_email': 'blabla@example.com',
            'assignee_id': 1000,
            'assignee_email': '',
            'task_data_interval_start': sample_time,
            'task_data_interval_end': sample_time
        }

    def object_to_dict(self, obj):
        return {
            c.name: getattr(obj, c.name).isoformat()
            if isinstance(getattr(obj, c.name), pendulum.DateTime)
            else getattr(obj, c.name)
            for c in obj.__table__.columns
        }

    def test__query_HS_clean_conversation(self):
        expected = [self.conv]
        result = self.session.query(HS_clean_conversation).all()
        self.assertEqual(result, expected)

    def test__query_HS_tag(self):
        expected = [self.tag]
        result = self.session.query(HS_tag).all()
        self.assertEqual(result, expected)

    def test__conv_id_not_nullable(self):
        result = self.session.query(HS_clean_conversation).all()
        assert_non_nullable(result[0], 'id')

    def test__tag_id_not_nullable(self):
        result = self.session.query(HS_tag).all()
        assert_non_nullable(result[0], 'id')

    def test__conv_number_nullable(self):
        result = self.session.query(HS_clean_conversation).all()
        assert_nullable(result[0], 'number')

    def test__tag_number_nullable(self):
        result = self.session.query(HS_tag).all()
        assert_nullable(result[0], 'name')

    def test__hs_clean_conversation_from_dict__base(self):
        self.maxDiff = None
        data, dict_tags = self.base_hs_data()
        start = '2022-01-01'
        end = '2022-01-01'
        hsconv = hs_clean_conversation_from_dict(data, dict_tags, start, end)

        expected = self.base_hs_conv()
        expected['task_data_interval_start'] = pendulum.parse(start).isoformat()
        expected['task_data_interval_end'] = pendulum.parse(end).isoformat()
        expected['id'] = None
        self.assertDictEqual(self.object_to_dict(hsconv), expected)

    def test__hs_clean_conversation_from_dict__customerWaitingSince_NoneTime(self):
        self.maxDiff = None
        data, dict_tags = self.base_hs_data()
        data['customerWaitingSince'] = {}
        start = '2022-01-01'
        end = '2022-01-01'
        hsconv = hs_clean_conversation_from_dict(data, dict_tags, start, end)

        expected = self.base_hs_conv()
        expected['task_data_interval_start'] = pendulum.parse(start).isoformat()
        expected['task_data_interval_end'] = pendulum.parse(end).isoformat()
        expected['id'] = None
        expected['customerWaitingSince_time'] = None
        self.assertDictEqual(self.object_to_dict(hsconv), expected)