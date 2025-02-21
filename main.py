from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, EmailStr, constr
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import datetime

# Database setup
DATABASE_URL = "sqlite:///./contacts.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Contact model
class Contact(Base):
    __tablename__ = "contacts"
    id = Column(Integer, primary_key=True, index=True)
    phoneNumber = Column(String, unique=False, nullable=True)
    email = Column(String, unique=False, nullable=True)
    linkedId = Column(Integer, ForeignKey("contacts.id"), nullable=True)
    linkPrecedence = Column(String, default="primary")
    createdAt = Column(DateTime, default=datetime.datetime.utcnow)
    updatedAt = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    deletedAt = Column(DateTime, nullable=True)

Base.metadata.create_all(bind=engine)

# Pydantic schema
class ContactRequest(BaseModel):
    email: EmailStr | None = None
    phoneNumber: constr(min_length=10, max_length=15) | None = None

# FastAPI instance
app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.post("/identify")
def identify_contact(request: ContactRequest, db: Session = Depends(get_db)):
    if not request.email and not request.phoneNumber:
        raise HTTPException(status_code=400, detail="At least one of email or phoneNumber is required")
    
    contacts = db.query(Contact).filter(
        (Contact.email == request.email) | (Contact.phoneNumber == request.phoneNumber)
    ).all()
    
    if not contacts:
        new_contact = Contact(email=request.email, phoneNumber=request.phoneNumber, linkPrecedence="primary")
        db.add(new_contact)
        db.commit()
        db.refresh(new_contact)
        return {
            "primaryContactId": new_contact.id,
            "emails": [new_contact.email] if new_contact.email else [],
            "phoneNumbers": [new_contact.phoneNumber] if new_contact.phoneNumber else [],
            "secondaryContactIds": []
        }
    
    primary_contact = next((c for c in contacts if c.linkPrecedence == "primary"), contacts[0])
    
    new_entries = []
    if request.email and request.email not in [c.email for c in contacts]:
        new_contact = Contact(email=request.email, linkedId=primary_contact.id, linkPrecedence="secondary")
        db.add(new_contact)
        new_entries.append(new_contact)
    if request.phoneNumber and request.phoneNumber not in [c.phoneNumber for c in contacts]:
        new_contact = Contact(phoneNumber=request.phoneNumber, linkedId=primary_contact.id, linkPrecedence="secondary")
        db.add(new_contact)
        new_entries.append(new_contact)
    
    db.commit()
    for entry in new_entries:
        db.refresh(entry)
    
    all_contacts = db.query(Contact).filter(
        (Contact.id == primary_contact.id) | (Contact.linkedId == primary_contact.id)
    ).all()
    
    return {
        "primaryContactId": primary_contact.id,
        "emails": list(set([c.email for c in all_contacts if c.email])),
        "phoneNumbers": list(set([c.phoneNumber for c in all_contacts if c.phoneNumber])),
        "secondaryContactIds": [c.id for c in all_contacts if c.linkPrecedence == "secondary"]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
