from sqlalchemy import Column, Integer, DateTime, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
Base = declarative_base()


class HS_clean_conversation(Base):
    __tablename__ = 'hs_clean_conversation'
    id = Column(Integer, primary_key=True)
    number = Column(Integer)
    id_helpscout = Column(Integer)
    threads = Column(Integer)
    type = Column(String) #tots son email?
    folderId = Column(Integer)
    status = Column(String)
    state = Column(String)
    subject = Column(String)
    mailboxId = Column(Integer)
    createdAt = Column(DateTime)
    closedBy = Column(Integer)
    closedAt = Column(DateTime)
    userUpdatedAt = Column(DateTime)
    cc = Column(String) #array de string
    bcc = Column(String) #array de string?
    createdBy_id = Column(Integer)
    createdBy_email = Column(String) #pel createdby (client) potser millor
    closedByUser_email = Column(String) #del created i closeb by tambe s'ha de pensar quins fan talta fotos no ?
    customerWaitingSince_time = Column(DateTime)
    source_type = Column(String)
    source_via = Column(String) #no se ben be que ens aporten els sources
    primaryCustomer_id = Column(Integer) #és el mateix que el creted by
    primaryCustomer_email = Column(String) #del customer tambe amb que ens quedem
    assignee_id = Column(Integer)
    assignee_email = Column(String) #de qui s'ha assginat potser podem retallar
    task_data_interval_start = Column(DateTime)
    task_data_interval_end = Column(DateTime)
    tags = relationship('HS_tag', secondary='Conversation_tag') #array de json?

class Conversation_tag(Base):
    __tablename__ = 'conversation_tag'
    clean_conversation_id = Column(Integer, ForeignKey('HS_clean_conversation.id'), primary_key = True)
    tag_id = Column(Integer, ForeignKey('HS_tag.id'), primary_key = True)

class HS_tag(Base):
   __tablename__ = 'hs_tag'
   id = Column(Integer, primary_key = True)
   created_at = Column(DateTime)
   name = Column(String)
   ticket_count = Column(Integer)
   clean_conversations = relationship(HS_clean_conversation,secondary='Conversation_tag')