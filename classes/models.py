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
    folder_id = Column(Integer)
    status = Column(String)
    state = Column(String)
    subject = Column(String)
    mailbox_id = Column(Integer)
    created_at = Column(DateTime)
    closed_by = Column(Integer)
    closed_at = Column(DateTime)
    user_updated_at = Column(DateTime)
    cc = Column(String)
    bcc = Column(String)
    created_by_id = Column(Integer)
    created_by_email = Column(String)
    closed_by_user_email = Column(String)
    customer_waiting_since_time = Column(DateTime)
    source_type = Column(String)
    source_via = Column(String)
    primary_customer_id = Column(Integer)
    primary_customer_email = Column(String)
    assignee_id = Column(Integer)
    assignee_email = Column(String)
    lang = Column(String)
    task_data_interval_start = Column(DateTime, index=True)
    task_data_interval_end = Column(DateTime, index=True)
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