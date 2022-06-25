from sqlalchemy import Column, Integer, DateTime, String, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
Base = declarative_base()

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

#Per crear les taules
#engine = create_engine()
#Base.metadata.create_all(engine)