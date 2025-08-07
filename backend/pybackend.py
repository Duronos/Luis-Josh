# EventPulse NC Backend - Main Application Structure
# Requirements: FastAPI, SQLAlchemy, PostgreSQL, Redis, Beautiful Soup, Scrapy

# requirements.txt
"""
fastapi==0.104.1
uvicorn==0.24.0
sqlalchemy==2.0.23
psycopg2-binary==2.9.9
alembic==1.13.1
redis==5.0.1
celery==5.3.4
beautifulsoup4==4.12.2
scrapy==2.11.0
requests==2.31.0
pydantic==2.5.0
python-multipart==0.0.6
python-jose[cryptography]==3.3.0
passlib[bcrypt]==1.7.4
aiofiles==23.2.1
httpx==0.25.2
geopy==2.4.1
python-dateutil==2.8.2
schedule==1.2.0
pandas==2.1.4
numpy==1.26.2
scikit-learn==1.3.2
nltk==3.8.1
"""

# ==================== DATABASE MODELS ====================
# models/database.py
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Text, Boolean, ForeignKey, Table
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid
from datetime import datetime

DATABASE_URL = "postgresql://user:password@localhost/eventpulse_nc"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Association tables for many-to-many relationships
event_categories = Table('event_categories',
    Base.metadata,
    Column('event_id', UUID(as_uuid=True), ForeignKey('events.id')),
    Column('category_id', Integer, ForeignKey('event_types.id'))
)

class Organization(Base):
    __tablename__ = "organizations"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, index=True)
    type = Column(String, nullable=False)  # university, government, organization
    website = Column(String)
    contact_info = Column(JSONB)
    timezone = Column(String, default="America/New_York")
    scraping_config = Column(JSONB)  # URLs, selectors, etc.
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    events = relationship("Event", back_populates="organization")

class EventType(Base):
    __tablename__ = "event_types"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    category = Column(String, nullable=False)  # academic, government, conference, etc.
    color_code = Column(String, default="#4f46e5")
    description = Column(Text)
    
    events = relationship("Event", secondary=event_categories, back_populates="types")

class Event(Base):
    __tablename__ = "events"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    title = Column(String, nullable=False, index=True)
    description = Column(Text)
    start_date = Column(DateTime, nullable=False, index=True)
    end_date = Column(DateTime, nullable=False)
    location_name = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    address = Column(String)
    registration_url = Column(String)
    contact_info = Column(JSONB)
    source_url = Column(String)
    is_recurring = Column(Boolean, default=False)
    recurrence_pattern = Column(JSONB)
    organization_id = Column(Integer, ForeignKey("organizations.id"))
    status = Column(String, default="active")  # active, cancelled, postponed
    capacity = Column(Integer)
    is_free = Column(Boolean, default=True)
    tags = Column(JSONB)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    organization = relationship("Organization", back_populates="events")
    types = relationship("EventType", secondary=event_categories, back_populates="events")

class ScrapingJob(Base):
    __tablename__ = "scraping_jobs"
    
    id = Column(Integer, primary_key=True, index=True)
    organization_id = Column(Integer, ForeignKey("organizations.id"))
    status = Column(String, default="pending")  # pending, running, completed, failed
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    events_found = Column(Integer, default=0)
    events_created = Column(Integer, default=0)
    events_updated = Column(Integer, default=0)
    error_message = Column(Text)
    log_data = Column(JSONB)

# ==================== PYDANTIC SCHEMAS ====================
# schemas/events.py
from pydantic import BaseModel, validator
from typing import List, Optional, Dict, Any
from datetime import datetime
from uuid import UUID

class OrganizationBase(BaseModel):
    name: str
    type: str
    website: Optional[str] = None
    contact_info: Optional[Dict[str, Any]] = None
    timezone: str = "America/New_York"

class OrganizationCreate(OrganizationBase):
    scraping_config: Optional[Dict[str, Any]] = None

class Organization(OrganizationBase):
    id: int
    is_active: bool
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True

class EventTypeBase(BaseModel):
    name: str
    category: str
    color_code: str = "#4f46e5"
    description: Optional[str] = None

class EventType(EventTypeBase):
    id: int
    
    class Config:
        from_attributes = True

class EventBase(BaseModel):
    title: str
    description: Optional[str] = None
    start_date: datetime
    end_date: datetime
    location_name: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    address: Optional[str] = None
    registration_url: Optional[str] = None
    contact_info: Optional[Dict[str, Any]] = None
    is_recurring: bool = False
    recurrence_pattern: Optional[Dict[str, Any]] = None
    capacity: Optional[int] = None
    is_free: bool = True
    tags: Optional[List[str]] = None

class EventCreate(EventBase):
    organization_id: int
    type_ids: List[int] = []

class Event(EventBase):
    id: UUID
    organization_id: int
    status: str
    source_url: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    organization: Organization
    types: List[EventType] = []
    
    class Config:
        from_attributes = True

class EventFilter(BaseModel):
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    organization_ids: Optional[List[int]] = None
    event_type_ids: Optional[List[int]] = None
    categories: Optional[List[str]] = None
    location: Optional[str] = None
    search_query: Optional[str] = None
    is_free: Optional[bool] = None

# ==================== WEB SCRAPING SYSTEM ====================
# scrapers/base_scraper.py
import asyncio
import aiohttp
from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import re
from geopy.geocoders import Nominatim
import logging
from urllib.parse import urljoin

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BaseScraper(ABC):
    def __init__(self, organization_id: int, config: Dict[str, Any]):
        self.organization_id = organization_id
        self.config = config
        self.session = None
        self.geocoder = Nominatim(user_agent="eventpulse_nc")
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    @abstractmethod
    async def scrape_events(self) -> List[Dict[str, Any]]:
        pass
    
    async def get_page_content(self, url: str) -> Optional[str]:
        try:
            async with self.session.get(url, timeout=30) as response:
                if response.status == 200:
                    return await response.text()
                logger.warning(f"Failed to fetch {url}: {response.status}")
                return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            return None
    
    def parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date formats commonly found on university websites"""
        date_patterns = [
            r'(\d{1,2})/(\d{1,2})/(\d{4})',  # MM/DD/YYYY
            r'(\d{4})-(\d{2})-(\d{2})',      # YYYY-MM-DD
            r'(\w+)\s+(\d{1,2}),?\s+(\d{4})', # Month DD, YYYY
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, date_str)
            if match:
                try:
                    if pattern == date_patterns[0]:  # MM/DD/YYYY
                        month, day, year = match.groups()
                        return datetime(int(year), int(month), int(day))
                    elif pattern == date_patterns[1]:  # YYYY-MM-DD
                        year, month, day = match.groups()
                        return datetime(int(year), int(month), int(day))
                    elif pattern == date_patterns[2]:  # Month DD, YYYY
                        month_name, day, year = match.groups()
                        month_map = {
                            'january': 1, 'february': 2, 'march': 3, 'april': 4,
                            'may': 5, 'june': 6, 'july': 7, 'august': 8,
                            'september': 9, 'october': 10, 'november': 11, 'december': 12
                        }
                        month = month_map.get(month_name.lower(), 1)
                        return datetime(int(year), month, int(day))
                except ValueError:
                    continue
        return None
    
    def geocode_location(self, location: str) -> tuple[Optional[float], Optional[float]]:
        """Geocode location to get latitude and longitude"""
        try:
            location_obj = self.geocoder.geocode(f"{location}, North Carolina, USA")
            if location_obj:
                return location_obj.latitude, location_obj.longitude
        except Exception as e:
            logger.error(f"Geocoding error for {location}: {str(e)}")
        return None, None

# scrapers/nc_state_scraper.py

class NCStateScraper(BaseScraper):
    async def scrape_events(self) -> List[Dict[str, Any]]:
        events = []
        base_urls = self.config.get('urls', [])

        for url in base_urls:
            content = await self.get_page_content(url)
            if not content:
                continue

            soup = BeautifulSoup(content, 'html.parser')
            event_selectors = self.config.get('selectors', {})

            # Find event containers for NC State calendar
            event_containers = soup.select(event_selectors.get('container', 'li.event_item'))

            for container in event_containers:
                try:
                    event_data = await self.extract_event_data(container, event_selectors)
                    if event_data:
                        events.append(event_data)
                except Exception as e:
                    logger.error(f"Error extracting event data: {str(e)}")
                    continue

        return events

    async def extract_event_data(self, container, selectors: Dict[str, str]) -> Optional[Dict[str, Any]]:
        # Use correct selectors for NC State event calendar
        title_elem = container.select_one(selectors.get('title', 'h3.event_title'))
        date_elem = container.select_one(selectors.get('date', 'span.event_date'))

        if not title_elem or not date_elem:
            return None

        title = title_elem.get_text(strip=True)
        date_str = date_elem.get_text(strip=True)

        # Clean up date string: Remove day-of-week and parse just "Month Day, Year" part
        # E.g., "Tue, Aug 6, 2024 9 am to 10 am" -> "Aug 6, 2024"
        date_clean = re.sub(r'^[A-Za-z]{3,9},?\s*', '', date_str)  # Remove weekday
        # Only keep date part (not time range)
        date_match = re.match(r'([A-Za-z]+ \d{1,2}, \d{4})', date_clean)
        date_base = date_match.group(1) if date_match else date_clean

        start_date = self.parse_date(date_base)
        if not start_date:
            return None

        # Extract other fields using NC State selectors
        description_elem = container.select_one(selectors.get('description', 'div.event_summary'))
        location_elem = container.select_one(selectors.get('location', 'span.event_location'))
        url_elem = container.select_one(selectors.get('url', 'a.event_link'))

        description = description_elem.get_text(strip=True) if description_elem else ""
        location = location_elem.get_text(strip=True) if location_elem else ""

        # Get absolute URL to event detail page
        source_url = url_elem.get('href') if url_elem else ""
        if source_url and not source_url.startswith("http"):
            source_url = urljoin("https://calendar.ncsu.edu/calendar", source_url)

        # Geocode location (optional)
        lat, lng = self.geocode_location(location) if location else (None, None)

        return {
            'title': title,
            'description': description,
            'start_date': start_date,
            'end_date': start_date + timedelta(hours=2),  # Default 2-hour duration
            'location_name': location,
            'latitude': lat,
            'longitude': lng,
            'source_url': source_url,
            'organization_id': self.organization_id,
            'tags': self.extract_tags(title, description)
        }

    def extract_tags(self, title: str, description: str) -> List[str]:
        """Extract relevant tags from title and description"""
        tags = []
        text = f"{title} {description}".lower()

        tag_keywords = {
            'workshop': ['workshop', 'hands-on', 'training'],
            'conference': ['conference', 'symposium', 'summit'],
            'seminar': ['seminar', 'lecture', 'presentation'],
            'meeting': ['meeting', 'session', 'committee'],
            'academic': ['research', 'academic', 'faculty', 'student'],
            'technology': ['tech', 'technology', 'computer', 'digital'],
            'health': ['health', 'medical', 'wellness'],
            'business': ['business', 'entrepreneur', 'startup']
        }

        for tag, keywords in tag_keywords.items():
            if any(keyword in text for keyword in keywords):
                tags.append(tag)

        return tags

# scrapers/unc_scraper.py
class UNCScraper(BaseScraper):
    async def scrape_events(self) -> List[Dict[str, Any]]:
        # Similar implementation for UNC system
        events = []
        # Implementation specific to UNC's website structure
        return events

# scrapers/duke_scraper.py
class DukeScraper(BaseScraper):
    async def scrape_events(self) -> List[Dict[str, Any]]:
        # Similar implementation for Duke
        events = []
        # Implementation specific to Duke's website structure
        return events

# scrapers/government_scraper.py
class GovernmentScraper(BaseScraper):
    async def scrape_events(self) -> List[Dict[str, Any]]:
        # Implementation for NC state government websites
        events = []
        # Scrape various state department calendars
        return events

# ==================== SCRAPING ORCHESTRATOR ====================
# services/scraping_service.py
from typing import Dict, Type
import asyncio
from datetime import datetime

class ScrapingOrchestrator:
    def __init__(self, db_session):
        self.db = db_session
        self.scrapers: Dict[str, Type[BaseScraper]] = {
            'nc_state': NCStateScraper,
            'unc': UNCScraper,
            'duke': DukeScraper,
            'government': GovernmentScraper
        }
    
    async def run_scraping_job(self, organization_id: int) -> Dict[str, Any]:
        """Run scraping job for a specific organization"""
        organization = self.db.query(Organization).filter(
            Organization.id == organization_id
        ).first()
        
        if not organization or not organization.is_active:
            return {'error': 'Organization not found or inactive'}
        
        # Create scraping job record
        job = ScrapingJob(
            organization_id=organization_id,
            status='running',
            started_at=datetime.utcnow()
        )
        self.db.add(job)
        self.db.commit()
        
        try:
            scraper_class = self.scrapers.get(organization.type)
            if not scraper_class:
                raise ValueError(f"No scraper available for type: {organization.type}")
            
            async with scraper_class(organization_id, organization.scraping_config) as scraper:
                events_data = await scraper.scrape_events()
                
                # Process and save events
                events_created = 0
                events_updated = 0
                
                for event_data in events_data:
                    event = await self.create_or_update_event(event_data)
                    if event:
                        if hasattr(event, '_created'):
                            events_created += 1
                        else:
                            events_updated += 1
                
                # Update job status
                job.status = 'completed'
                job.completed_at = datetime.utcnow()
                job.events_found = len(events_data)
                job.events_created = events_created
                job.events_updated = events_updated
                
        except Exception as e:
            job.status = 'failed'
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            logger.error(f"Scraping job failed for org {organization_id}: {str(e)}")
        
        self.db.commit()
        
        return {
            'job_id': job.id,
            'status': job.status,
            'events_found': job.events_found,
            'events_created': job.events_created,
            'events_updated': job.events_updated
        }
    
    async def create_or_update_event(self, event_data: Dict[str, Any]) -> Optional[Event]:
        """Create new event or update existing one"""
        # Check if event already exists (by title, date, and organization)
        existing_event = self.db.query(Event).filter(
            Event.title == event_data['title'],
            Event.start_date == event_data['start_date'],
            Event.organization_id == event_data['organization_id']
        ).first()
        
        if existing_event:
            # Update existing event
            for key, value in event_data.items():
                if hasattr(existing_event, key):
                    setattr(existing_event, key, value)
            existing_event.updated_at = datetime.utcnow()
            return existing_event
        else:
            # Create new event
            new_event = Event(**event_data)
            new_event._created = True  # Mark as created for tracking
            self.db.add(new_event)
            return new_event

# ==================== API ENDPOINTS ====================
# main.py
from fastapi import FastAPI, Depends, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Optional
import redis
import json
from datetime import datetime, timedelta

app = FastAPI(title="EventPulse NC API", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Redis for caching
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/")
async def root():
    return {"message": "EventPulse NC API", "version": "1.0.0"}

@app.get("/api/events", response_model=List[Event])
async def get_events(
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    organization_ids: Optional[str] = Query(None),
    categories: Optional[str] = Query(None),
    search: Optional[str] = Query(None),
    limit: int = Query(100, le=1000),
    offset: int = Query(0),
    db: Session = Depends(get_db)
):
    """Get events with filtering options"""
    
    # Create cache key
    cache_key = f"events:{start_date}:{end_date}:{organization_ids}:{categories}:{search}:{limit}:{offset}"
    
    # Try to get from cache first
    cached_result = redis_client.get(cache_key)
    if cached_result:
        return json.loads(cached_result)
    
    query = db.query(Event).filter(Event.status == 'active')
    
    # Apply filters
    if start_date:
        query = query.filter(Event.start_date >= start_date)
    if end_date:
        query = query.filter(Event.end_date <= end_date)
    if organization_ids:
        org_ids = [int(id) for id in organization_ids.split(',')]
        query = query.filter(Event.organization_id.in_(org_ids))
    if search:
        query = query.filter(Event.title.ilike(f'%{search}%'))
    
    # Execute query
    events = query.offset(offset).limit(limit).all()
    
    # Cache result for 15 minutes
    redis_client.setex(cache_key, 900, json.dumps([event.dict() for event in events]))
    
    return events

@app.get("/api/events/{event_id}", response_model=Event)
async def get_event(event_id: str, db: Session = Depends(get_db)):
    """Get single event by ID"""
    event = db.query(Event).filter(Event.id == event_id).first()
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    return event

@app.get("/api/organizations", response_model=List[Organization])
async def get_organizations(db: Session = Depends(get_db)):
    """Get all organizations"""
    return db.query(Organization).filter(Organization.is_active == True).all()

@app.get("/api/event-types", response_model=List[EventType])
async def get_event_types(db: Session = Depends(get_db)):
    """Get all event types"""
    return db.query(EventType).all()

@app.get("/api/stats")
async def get_stats(db: Session = Depends(get_db)):
    """Get application statistics"""
    cache_key = "stats:overview"
    cached_stats = redis_client.get(cache_key)
    
    if cached_stats:
        return json.loads(cached_stats)
    
    total_events = db.query(Event).filter(Event.status == 'active').count()
    total_organizations = db.query(Organization).filter(Organization.is_active == True).count()
    
    # Events this week
    week_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    week_start -= timedelta(days=week_start.weekday())
    week_end = week_start + timedelta(days=7)
    
    events_this_week = db.query(Event).filter(
        Event.start_date >= week_start,
        Event.start_date < week_end,
        Event.status == 'active'
    ).count()
    
    # Events today
    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)
    
    events_today = db.query(Event).filter(
        Event.start_date >= today_start,
        Event.start_date < today_end,
        Event.status == 'active'
    ).count()
    
    stats = {
        'total_events': total_events,
        'total_organizations': total_organizations,
        'events_this_week': events_this_week,
        'events_today': events_today
    }
    
    # Cache for 5 minutes
    redis_client.setex(cache_key, 300, json.dumps(stats))
    
    return stats

@app.post("/api/scraping/trigger")
async def trigger_scraping(
    background_tasks: BackgroundTasks,
    organization_id: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """Trigger scraping job for specific organization or all organizations"""
    orchestrator = ScrapingOrchestrator(db)
    
    if organization_id:
        background_tasks.add_task(orchestrator.run_scraping_job, organization_id)
        return {"message": f"Scraping job triggered for organization {organization_id}"}
    else:
        # Trigger for all active organizations
        organizations = db.query(Organization).filter(Organization.is_active == True).all()
        for org in organizations:
            background_tasks.add_task(orchestrator.run_scraping_job, org.id)
        
        return {"message": f"Scraping jobs triggered for {len(organizations)} organizations"}

@app.get("/api/heatmap")
async def get_heatmap_data(
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    db: Session = Depends(get_db)
):
    """Get heatmap data for specified date range"""
    
    cache_key = f"heatmap:{start_date.date()}:{end_date.date()}"
    cached_data = redis_client.get(cache_key)
    
    if cached_data:
        return json.loads(cached_data)
    
    # Query events in date range
    events = db.query(Event).filter(
        Event.start_date >= start_date,
        Event.start_date <= end_date,
        Event.status == 'active'
    ).all()
    
    # Create heatmap data structure (7 days x 24 hours)
    heatmap_data = {}
    
    for event in events:
        day_key = event.start_date.strftime('%Y-%m-%d')
        hour_key = event.start_date.hour
        
        if day_key not in heatmap_data:
            heatmap_data[day_key] = {}
        
        if hour_key not in heatmap_data[day_key]:
            heatmap_data[day_key][hour_key] = 0
        
        heatmap_data[day_key][hour_key] += 1
    
    # Cache for 1 hour
    redis_client.setex(cache_key, 3600, json.dumps(heatmap_data))
    
    return heatmap_data

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)