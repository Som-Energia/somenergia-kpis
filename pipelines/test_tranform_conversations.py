import unittest

from sqlalchemy_utils import (
    assert_nullable,
    assert_non_nullable,
)
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
from sqlalchemy import Column, Integer, DateTime, String, ForeignKey, create_engine
from sqlalchemy.orm import relationship

class HS_clean_conversation(Base):
    __tablename__ = 'hs_clean_conversation'
    id = Column(Integer, primary_key=True)
    number = Column(Integer)
    id_helpscout = Column(Integer)
    threads = Column(Integer)
    type = Column(String)
    folderId = Column(Integer)
    status = Column(String)
    state = Column(String)
    subject = Column(String)
    mailboxId = Column(Integer)
    createdAt = Column(DateTime)
    closedBy = Column(Integer)
    closedAt = Column(DateTime)
    userUpdatedAt = Column(DateTime)
    cc = Column(String)
    bcc = Column(String)
    createdBy_id = Column(Integer)
    createdBy_email = Column(String)
    closedByUser_email = Column(String)
    customerWaitingSince_time = Column(DateTime)
    source_type = Column(String)
    source_via = Column(String)
    primaryCustomer_id = Column(Integer)
    primaryCustomer_email = Column(String)
    assignee_id = Column(Integer)
    assignee_email = Column(String)
    task_data_interval_start = Column(DateTime)
    task_data_interval_end = Column(DateTime)
    tags = relationship('HS_tag', secondary='hs_conversation_tag')

class HS_tag(Base):
   __tablename__ = 'hs_tag'
   id = Column(Integer, primary_key = True)
   name = Column(String)
   clean_conversations = relationship(HS_clean_conversation, secondary='hs_conversation_tag')

class Conversation_tag(Base):
    __tablename__ = 'hs_conversation_tag'
    clean_conversation_id = Column(Integer, ForeignKey('hs_clean_conversation.id'), primary_key = True)
    tag_id = Column(Integer, ForeignKey('hs_tag.id'), primary_key = True)

engine = create_engine('sqlite:///:memory:')
Base.metadata.create_all(engine)

class HelpscoutTransformTest(unittest.TestCase):

    Session = sessionmaker(bind=engine)
    session = Session()

    def setUp(self):
        Base.metadata.create_all(engine)
        self.tag = HS_tag(id=1, name='test_tag')
        self.session.add(self.tag)
        self.conv = HS_clean_conversation(id=1, number=2, id_helpscout=3, customerWaitingSince_time=None, tags=[self.tag])
        self.session.add(self.conv)

        self.session.commit()

    def tearDown(self):
        Base.metadata.drop_all(engine)

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
