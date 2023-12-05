import os
import csv
import logging
from termcolor import colored
from sqlalchemy import (
    Column, ForeignKey, Integer, 
    String, text, create_engine)
from sqlalchemy.orm import (
    declarative_base, joinedload, 
    relationship, sessionmaker)

class ColoredFormatter(logging.Formatter):
    COLORS = {
        'WARNING': 'yellow',
        'INFO': 'green',
        'DEBUG': 'blue',
        'CRITICAL': 'red',
        'ERROR': 'red'
    }

    def format(self, record):
        log_message = super(ColoredFormatter, self).format(record)
        return colored(log_message, self.COLORS.get(record.levelname))

logger = logging.getLogger(__name__)
logger.propagate = False

handler = logging.StreamHandler()
formatter = ColoredFormatter("[%(levelname)s] - %(message)s")
handler.setFormatter(formatter)

logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

Base = declarative_base()

class Card(Base):
    """
    First extrapolation layer
    Represents a TCGplayer SKU + quantity
    Quantity not to exceed 40
    """
    __tablename__ = 'cards'

    id = Column(Integer, primary_key=True)
    section_id = Column(Integer, ForeignKey('sections.id'))
    tcg_id = Column(Integer, nullable=False)
    card_name = Column(String)
    set_name = Column(String)
    quantity = Column(Integer, default=0)
    max_quantity = Column(Integer, default=40)    
    
    sleeve = relationship("Section", back_populates="cards")

class Section(Base):
    """
    """
    __tablename__ = 'sections'
    
    id = Column(Integer, primary_key=True)
    row_id = Column(Integer, ForeignKey('rows.id'))
    card_count = Column(Integer, default=0)
    max_card_quantity = Column(Integer, default=100)

    row = relationship("Row", back_populates="sections")

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


MAX_CARD_QUANTITY = 20  # Maximum quantity for a unique card SKU
MAX_SECTION_CARDS = 100  # Maximum cards a section can hold
MAX_ROW_CARDS = 1000  # Maximum cards a row can hold (or 10 full sections)
MAX_ROW_SECTIONS = 10  # Maximum sections a row can hold
MAX_BOX_ROWS = 5  # Maximum rows a box can hold


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

def add_card_to_section(session, section, card):
    if section.card_count >= MAX_SECTION_CARDS:
        return False

    remaining_capacity = MAX_SECTION_CARDS - section.current_quantity
    add_quantity = min(remaining_capacity, card.quantity)

    if add_quantity > 0:
        section.current_quantity += add_quantity
        section.card_count += 1
        card.section_id = section.id
        session.add(card)
        session.flush()
        return True

    return False

def find_or_create_storage(session, parent, child_class, attr):
    available_storage = [
        item for item in getattr(parent, attr) 
        if getattr(item, "current_quantity", 0) + 1 <= getattr(item, "max_capacity", 0)
    ]
    
    if available_storage:
        return available_storage[0]
    
    if len(getattr(parent, attr)) < getattr(parent, f"max_{attr}"):
        new_storage = child_class(parent_id=parent.id)
        session.add(new_storage)
        session.flush()
        return new_storage

    return None

def prepare_card(tcg_id, card_name, set_name, quantity, section_id=None):
    new_card = Card(
        tcg_id=tcg_id, 
        card_name=card_name, 
        set_name=set_name, 
        quantity=quantity, 
        section_id=section_id)
    return new_card

def add_card_to_section(session, section, tcg_id, card_name, set_name, quantity):
    new_card = prepare_card(tcg_id, card_name, set_name, quantity, section.id)
    section.current_quantity += quantity
    section.card_count += 1
    session.add(new_card)
    session.flush()

def find_available_section(session):
    return session.query(Section).filter(
        Section.current_quantity + 1 <= MAX_SECTION_CARDS
    ).first()

def create_storage_object(session, parent, child_class, attr, max_attr):
    available_storage = next(
        (item for item in getattr(parent, attr) if item.current_quantity + 1 <= item.max_capacity),
        None
    )
    
    if available_storage:
        return available_storage
    
    if len(getattr(parent, attr)) < getattr(parent, max_attr):
        new_storage = child_class(parent_id=parent.id)
        session.add(new_storage)
        return new_storage
    
    return None

def insert_card(session, tcg_id, card_name, set_name, quantity):
    section = find_available_section(session)
    
    if section and add_card_to_section(session, section, Card):
        return
    
    box_list = session.query(Box).all()
    for box in box_list:
        for row in box.rows:
            for section in row.sections:
                if add_card_to_section(session, section, Card):
                    return
            
            new_section = create_storage_object(session, row, Section, 'sections', 'max_sections')
            if new_section and add_card_to_section(session, new_section, Card):
                return
    
        new_row = create_storage_object(session, box, Row, 'rows', 'max_rows')
        if new_row:
            new_section = create_storage_object(session, new_row, Section, 'sections', 'max_sections')
            if new_section and add_card_to_section(session, new_section, Card):
                return
    
    # Creating new Box, Row, and Section as the last resort
    new_box = Box()
    new_row = Row(box_id=new_box.id)
    new_section = Section(row_id=new_row.id)
    
    session.add_all([new_box, new_row, new_section])
    add_card_to_section(session, new_section, Card)

    session.commit()

def upload_from_csv(filename, session):
    with open(filename, 'r') as csvfile:
        card_reader = csv.DictReader(csvfile)
        for row in card_reader:
            try:
                tcg_id = int(row['TCGplayer Id'])
                card_name = row['Product Name']
                set_name = row['Set Name']
                quantity = int(row['Add to Quantity'])

                print(f"TCG ID: {tcg_id}, Name: {card_name}, Set: {set_name}, Quantity: {quantity} added to database")
                insert_card(session, tcg_id, card_name, set_name, quantity)
                session.commit()
            except ValueError:
                logger.warning(f"Invalid TCGplayer Id: {row['TCGplayer Id']}. Skipping row.")
                continue

def match_order_from_csv(filename, session):
    to_remove_list = []
    output = []

    with open(filename, 'r') as csvfile:
        card_reader = csv.DictReader(csvfile)
        for row in card_reader:
            try:
                tcg_id = int(row['TCGplayer Id'])
                card_name = row['Product Name']
                set_name = row['Set Name']
                quantity = int(row['Add to Quantity'])
                locations, cards_to_remove = find_card_location(session, tcg_id, quantity)
                if locations:
                    to_remove_list.extend(cards_to_remove)
                    output.append({"TCGplayer Id": tcg_id, "Product Name": card_name, "Set Name": set_name, "Quantity": quantity, "Locations": locations})

            except ValueError:
                print(f"Skipping row with invalid TCGplayer Id: {row['TCGplayer Id']}")
                continue 
    remove_cards(session, to_remove_list)

    if output:
        keys = output[0].keys()
        filename, file_extension = os.path.splitext(filename)
        location_filename = f"{filename}-locations{file_extension}"
        log_filename = f"{filename}-log.txt"
        
        with open(location_filename, 'w') as output_file:
            dict_writer = csv.DictWriter(output_file, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(output)

        with open(log_filename, 'a') as log_file:
            for item in output:
                log_file.write(f"Removed card with TCG Id: {item['TCGplayer Id']}, Location: {item['Locations']}\n")
    else:
        print("No valid output to save.")

def find_card_location(session, tcg_id, needed_quantity):
    locations_with_counts = []
    cards_to_remove = []

    query_result = session.query(Box, Row, Section, Card).filter(
        Box.id == Row.box_id,
        Row.id == Section.row_id,
        Section.id == Card.id,
        Card.tcg_id == tcg_id
    ).all()

    total_cards_collected = 0

    for box, row, section, sleeve, card in query_result:
        if total_cards_collected >= needed_quantity:
            break

        location_str = f"{box.id}.{row.id}.{section.id}.{sleeve.id}"
        
        location_dict = next((loc for loc in locations_with_counts if loc['Location'] == location_str), None)

        if location_dict is None:
            location_dict = {'Location': location_str, 'Card Count': 0}
            locations_with_counts.append(location_dict)

        cards_to_collect_here = min(card.quantity, needed_quantity - total_cards_collected)
        
        location_dict['Card Count'] += cards_to_collect_here
        total_cards_collected += cards_to_collect_here

        cards_to_remove.append({'card': card, 'quantity_to_remove': cards_to_collect_here})

    return locations_with_counts, cards_to_remove

def remove_cards(session, cards_to_remove):
    for card in cards_to_remove:
        session.delete(card)
    session.commit()


if __name__ == "__main__":
    engine = create_engine('sqlite:///mtg_inventory.db')
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    session = Session()

    filepath = r'/home/elmo/mtg-inv-sys/roca-test-3.csv'
    upload_from_csv(filepath, session)

    # pull order/RI from database
    #filepath = r'/home/elmo/mtg-inv-sys/roca-test-2.csv'
    #match_order_from_csv(filepath, session)