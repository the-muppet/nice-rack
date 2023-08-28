import csv
import logging
from sqlalchemy import (
    Column, ForeignKey, Integer, 
    String, create_engine)
from sqlalchemy.orm import (
    declarative_base, joinedload, 
    relationship, sessionmaker)

logging.basicConfig(level=logging.INFO)
Base = declarative_base()


class Card(Base):
    """
    First extrapolation layer
    Represents a TCGplayer SKU + quantity
    Quantity not to exceed 40
    """
    __tablename__ = 'cards'

    id = Column(Integer, primary_key=True)
    sleeve_id = Column(Integer, ForeignKey('sleeves.id'))
    tcg_id = Column(Integer, nullable=False)
    card_name = Column(String)
    set_name = Column(String)
    quantity = Column(Integer, default=0)
    max_quantity = Column(Integer, default=40)    
    
    sleeve = relationship("Sleeve", back_populates="cards")


class Sleeve(Base):
    """
    Second extrapolation layer
    Represents a group of Card objects
    Total quantity not to exceed 12
    May contain different Cards 
    """
    __tablename__ = 'sleeves'
    
    id = Column(Integer, primary_key=True)
    section_id = Column(Integer, ForeignKey('sections.id'))
    card_count = Column(Integer, default=0)
    max_cards = Column(Integer, default=12)
    current_quantity = Column(Integer, default=0)

    section = relationship("Section", back_populates="sleeves")
    cards = relationship("Card", back_populates="sleeve")

    def can_add_card(self, quantity):
        return self.current_quantity + quantity <= self.max_cards

    def add_card(self, card):
        if self.card_count < self.max_cards:
            self.cards.append(card)
            self.card_count += 1
            return True
        return False
    

class Section(Base):
    """
    Third layer in system
    Represents a cluster of Sleeves as a sub-section of a Row
    10 Sections produce a full Row storage object.
    """
    __tablename__ = 'sections'
    
    id = Column(Integer, primary_key=True)
    row_id = Column(Integer, ForeignKey('rows.id'))
    sleeve_count = Column(Integer, default=0)
    max_sleeves = Column(Integer, default=10)

    row = relationship("Row", back_populates="sections")
    sleeves = relationship("Sleeve", back_populates="section")

    def add_sleeve(self, sleeve):
        if self.sleeve_count < self.max_sleeves:
            self.sleeves.append(sleeve)
            self.sleeve_count += 1
            return True
        return False


class Row(Base):
    """
    Fourth layer of system
    Storage object housing 10 Sections
    5 Rows together form a Box and do not move once created
    """
    __tablename__ = 'rows'
    
    id = Column(Integer, primary_key=True)
    box_id = Column(Integer, ForeignKey('boxes.id'))
    section_count = Column(Integer, default=0)
    max_sections = Column(Integer, default=10)

    box = relationship("Box", back_populates="rows")
    sections = relationship("Section", back_populates="row")

    def add_section(self, section):
        if self.section_count < self.max_sections:
            self.sections.append(section)
            self.section_count += 1
            return True
        return False


class Box(Base):
    """
    Top layer of inventory management system
    A Box represents an entire Row.Section.Sleeve.Card inventory segment
    Any number of Boxes may be created and/or destroyed as needed.
    """
    __tablename__ = 'boxes'
    
    id = Column(Integer, primary_key=True)
    name = Column(String)
    location = Column(String)
    row_count = Column(Integer, default=0)
    max_rows = Column(Integer, default=5)

    rows = relationship("Row", back_populates="box")

    def add_row(self, row):
        if self.row_count < self.max_rows:
            self.rows.append(row)
            self.row_count += 1
            return True
        return False
    

def calculate_box_location(total_boxes):
    boxes_per_column = 3
    columns_per_shelf = 4
    shelves_per_rack = 3
    boxes_per_shelf = boxes_per_column * columns_per_shelf
    boxes_per_rack = boxes_per_shelf * shelves_per_rack

    rack_number = total_boxes // boxes_per_rack + 1
    shelf_number = (total_boxes % boxes_per_rack) // boxes_per_shelf + 1
    column_number = ((total_boxes % boxes_per_rack) % boxes_per_shelf) // boxes_per_column + 1
    box_number = ((total_boxes % boxes_per_rack) % boxes_per_shelf) % boxes_per_column + 1

    return rack_number, shelf_number, column_number, box_number


def find_or_create_storage(session, parent, child_class, attr):
    """Finds available storage or creates new one if necessary."""
    available_storage = [item for item in getattr(parent, attr) if len(item.cards) < item.max_cards]
    
    if available_storage:
        return available_storage[0]

    if len(getattr(parent, attr)) < getattr(parent, f"max_{attr}"):
        new_storage = child_class()
        session.add(new_storage)
        getattr(parent, attr).append(new_storage)
        session.flush()
        return new_storage
    return None


def prepare_card(tcg_id, card_name, set_name, quantity, sleeve_id=None):
    new_card = Card(tcg_id=tcg_id, card_name=card_name, set_name=set_name, quantity=quantity, sleeve_id=sleeve_id)
    return new_card


def add_card_to_sleeve(session, sleeve, tcg_id, card_name, set_name, quantity):
    new_card = prepare_card(tcg_id, card_name, set_name, quantity, sleeve.id)
    sleeve.current_quantity += quantity
    sleeve.card_count += 1
    session.add(new_card)
    session.flush()


def insert_card(session, tcg_id, card_name, set_name, quantity):
    logging.info(f"Inserting TCG ID: {tcg_id}, Quantity: {quantity}")
    
    sleeve = session.query(Sleeve).filter(Sleeve.current_quantity + quantity <= Sleeve.max_cards).first()
    
    if sleeve:
        add_card_to_sleeve(session, sleeve, tcg_id, card_name, set_name, quantity)
        return
    
    for box in session.query(Box).options(
        joinedload(Box.rows).
        joinedload(Row.sections).
        joinedload(Section.sleeves)
        ).all():
        for row in box.rows:
            for section in row.sections:
                sleeve = find_or_create_storage(session, section, Sleeve, 'sleeves')
                if sleeve:
                    add_card_to_sleeve(session, sleeve, tcg_id, card_name, set_name, quantity)
                    return
                
                new_section = find_or_create_storage(session, row, Section, 'sections')
                if new_section:
                    sleeve = find_or_create_storage(session, new_section, Sleeve, 'sleeves')
                    if sleeve:
                        add_card_to_sleeve(session, sleeve, tcg_id, card_name, set_name, quantity)
                        return
                    
            new_row = find_or_create_storage(session, box, Row, 'rows')
            if new_row:
                section = find_or_create_storage(session, new_row, Section, 'sections')
                if section:
                    sleeve = find_or_create_storage(session, section, Sleeve, 'sleeves')
                    if sleeve:
                        add_card_to_sleeve(session, sleeve, tcg_id, card_name, set_name, quantity)
                        return
    
    # If code reaches here, create new Box, Row, Section, Sleeve, and add Card
    total_boxes = session.query(Box).count()
    rack_number, shelf_number, column_number, box_number = calculate_box_location(total_boxes)
    new_box = Box(name=f"Rack {rack_number}, Shelf {shelf_number}, Column {column_number}, Box {box_number}",
                  location=f"Shelf {shelf_number}, Column {column_number}")
    
    session.add(new_box)
    session.flush()
    
    new_row = Row(box_id=new_box.id)
    new_section = Section(row_id=new_row.id)
    new_sleeve = Sleeve(section_id=new_section.id)
    new_card = prepare_card(tcg_id, card_name, set_name, quantity, new_sleeve.id)
    session.add_all([new_row, new_section, new_sleeve, new_card])
    session.flush()

def upload_from_csv(filename, session):
    with open(filename, 'r') as csvfile:
        card_reader = csv.DictReader(csvfile)
        for row in card_reader:
            tcg_id = int(row['TCGplayer Id'])
            card_name = row['Product Name']
            set_name = row['Set Name']
            quantity = int(row['Add to Quantity'])
            print(f"TCG ID: {tcg_id}, Name: {card_name}, Set: {set_name}, Quantity: {quantity} added to database")
            insert_card(session, tcg_id, card_name, set_name, quantity)

def query_inventory(session, tcg_id):
    return session.query(Section).filter(
        Section.sleeves.any(Sleeve.cards.any(Card.tcg_id == tcg_id))
    ).all()

def query_inventory_by_name(session, partial_name):
    return session.query(Section).filter(
        Section.sleeves.any(Sleeve.cards.any(Card.name.ilike(f"%{partial_name}%")))
    ).all()

def query_inventory_by_set(session, partial_set):
    return session.query(Section).filter(
        Section.sleeves.any(Sleeve.cards.any(Card.name.ilike(f"%{partial_set}%")))
    ).all()

if __name__ == "__main__":
    engine = create_engine('sqlite:///mtg_inventory.db')
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    session = Session()

    filepath = r'/home/elmo/mtg-inv-sys/roca-test-1.csv'
    upload_from_csv(filepath, session)