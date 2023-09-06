import argparse
import json
import csv
import logging
import os
from datetime import datetime

from sqlalchemy import Column, ForeignKey, Integer, String, create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, declarative_base, relationship, sessionmaker
from termcolor import colored

Base = declarative_base()

MAX_ROWS = 5
MAX_SECTIONS_PER_ROW = 10
MAX_CARDS_PER_SECTION = 1200


class ColoredFormatter(logging.Formatter):
    COLORS = {
        "WARNING": "yellow",
        "INFO": "green",
        "DEBUG": "blue",
        "CRITICAL": "red",
        "ERROR": "red",
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


class Card(Base):
    """
    Database Object representing cards existance in system.
    holds its name,set, quantity, etc.
    """

    __tablename__ = "cards"

    id = Column(Integer, primary_key=True)
    section_id = Column(Integer, ForeignKey("sections.id"))
    tcg_id = Column(Integer, nullable=False)
    card_name = Column(String)
    set_name = Column(String)
    quantity = Column(Integer, default=0)
    capacity = Column(Integer, default=12)

    section = relationship("Section", back_populates="cards")

    def to_dict(self):
        return {
            "TCGplayer Id": self.tcg_id,
            "Product Name": self.card_name,
            "Set Name": self.set_name,
            "Add to Quantity": self.quantity,
        }


class Section(Base):
    """
    Holds Cards - each section has a max quantity of 100
    """

    __tablename__ = "sections"

    id = Column(Integer, primary_key=True)
    row_id = Column(Integer, ForeignKey("rows.id"))
    card_count = Column(Integer, default=0)
    max_cards = Column(Integer, default=12)
    current_quantity = Column(Integer, default=0)

    row = relationship("Row", back_populates="sections")
    cards = relationship("Card", back_populates="section")

    def can_add_card(self, quantity):
        return (self.card_count + quantity) <= self.max_cards

    def add_card(self, card):
        if self.can_add_card(card.quantity):
            self.cards.append(card)
            self.card_count += card.quantity
            return True
        return False

    def actual_quantity(self):
        return sum(card.quantity for card in self.cards)

    def to_dict(self):
        return {
                "card_count": self.card_count,
                "current_quantity": self.current_quantity,
                "cards": [card.to_dict() for card in self.cards],
            }


class Row(Base):
    """
    contains 10 Sections, 5 Rows constitute a box.
    """

    __tablename__ = "rows"

    id = Column(Integer, primary_key=True)
    box_id = Column(Integer, ForeignKey("boxes.id"))
    section_count = Column(Integer, default=0)
    max_sections = Column(Integer, default=10)

    box = relationship("Box", back_populates="rows")
    sections = relationship("Section", back_populates="row")

    def __init__(self, *args, **kwargs):
        super(Row, self).__init__(*args, **kwargs)
        self.max_sections = self.max_sections if self.max_sections is not None else 10
        self.section_count = self.section_count if self.section_count is not None else 0

    def to_dict(self):
        return {
            "sections": [section.to_dict() for section in self.sections]
        }

    def add_section(self, section):
        if self.section_count < self.max_sections:
            self.sections.append(section)
            self.section_count += 1
            return True
        return False


class Box(Base):
    """
    Top level inventory management system
    Any number of Boxes may be created and/or destroyed as needed.
    """

    __tablename__ = "boxes"

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

    def __init__(self, *args, **kwargs):
        super(Box, self).__init__(*args, **kwargs)
        self.max_rows = self.max_rows if self.max_rows is not None else 5
        self.row_count = self.row_count if self.row_count is not None else 0
        self.rows = []
        for _ in range(self.max_rows):
            new_row = Row()
            self.add_row(new_row)
            for _ in range(new_row.max_sections):
                new_section = Section()
                new_row.add_section(new_section)

    def to_dict(self):
        return {
            "rows": [row.to_dict() for row in self.rows]
            }


class InventoryStatus:
    def __init__(self):
        self.current_box = None
        self.current_row = None
        self.current_section = None
        self.last_insertion_date = None
        self.last_removal_date = None
        self.total_card_count = 0
        self.total_section_count = 0
        self.total_row_count = 0
        self.total_box_count = 0

    def update_after_insertion(self, current_box, current_row, current_section):
        self.current_box = current_box
        self.current_row = current_row
        self.current_section = current_section
        self.last_insertion_date = datetime.now()


def locate_insertion_point(session, inventory_status, tcg_id, quantity):
    logger.debug("Entering locate_insertion_point")

    last_box = session.query(Box).order_by(Box.id.desc()).first()
    logger.debug(f"Last box: {last_box}")

    if not last_box:
        last_box = Box()
        session.add(last_box)
        session.flush()
        logger.debug("Created a new box")

    for row in last_box.rows:
        logger.debug(f"Checking row: {row}")
        if row.sections is not None:
            for section in row.sections:
                logging.debug(f"Checking section: {section}")
                if section.can_add_card(12):
                    current_section = section
                    current_row = row
                    logger.debug(f"Located suitable section: {current_section}")
                    return current_section
                else:
                    logger.debug(f"Section is full: {section}")

    new_box = Box()
    for _ in range(5):
        new_row = Row()
        new_box.add_row(new_row)
        for _ in range(10):
            new_section = Section()
            new_row.add_section(new_section)

    session.add(new_box)
    session.flush()
    logger.debug(f"Created a new box: {new_box}")

    return new_box.rows[0].sections[0]


def prepare_card(tcg_id, card_name, set_name, quantity, section_id=None):
    new_card = Card(
        tcg_id=tcg_id,
        card_name=card_name,
        set_name=set_name,
        quantity=quantity,
        section_id=section_id,
    )
    return new_card


def insert_card(
    session: Session,
    inventory_status: InventoryStatus,
    tcg_id,
    card_name,
    set_name,
    quantity,
):
    section = locate_insertion_point(session, inventory_status, tcg_id, quantity)
    if section:
        new_card = Card(
            tcg_id=tcg_id, card_name=card_name, set_name=set_name, quantity=quantity
        )
        new_card.section_id = section.id

        if section.add_card(new_card):
            session.add(section)
            session.commit()
            logger.debug("Successfully added card to section.")
            section.current_quantity += quantity

            inventory_status.update_after_insertion(
                inventory_status.current_box, inventory_status.current_row, section
            )
            session.add(new_card)
            try:
                session.commit()
            except Exception as e:
                logger.error(f"Failed to commit session: {e}")
                session.rollback()

            print(f"Successfully added {card_name} to inventory.")
        else:
            logger.debug("Failed to add card to section.")
            print(f"Failed to insert {card_name}. Open section not found.")
    else:
        print(f"Failed to insert {card_name}. Open section not found.")


def update_card_quantity(
    session: Session, inventory_status: InventoryStatus, tcg_id, additional_quantity
):
    existing_card = session.query(Card).filter_by(tcg_id=tcg_id).first()

    if existing_card:
        new_quantity = existing_card.quantity + additional_quantity

        if new_quantity > existing_card.max_quantity:
            spillover_quantity = new_quantity - existing_card.max_quantity

            existing_card.quantity = existing_card.max_quantity

            insert_card(
                session,
                inventory_status,
                tcg_id,
                existing_card.card_name,
                existing_card.set_name,
                spillover_quantity,
            )
        else:
            existing_card.quantity = new_quantity

        session.commit()
        print(f"Successfully updated the quantity of {existing_card.card_name}.")
    else:
        print(f"Card with TCG ID {tcg_id} not found.")


def upload_from_csv(filepath, session, inventory_status):
    row_count = 0
    success_count = 0
    with open(filepath, "r") as csvfile:
        card_reader = csv.DictReader(csvfile)
        for row in card_reader:
            row_count += 1
            try:
                tcg_id = int(row["TCGplayer Id"])
                card_name = row["Product Name"]
                set_name = row["Set Name"]
                quantity = int(row["Add to Quantity"])

                print(f"{quantity} || {card_name} || {set_name} || {tcg_id},")

                if insert_card(
                    session, inventory_status, tcg_id, card_name, set_name, quantity
                ):
                    success_count += 1

            except ValueError:
                logger.warning(
                    f"Invalid TCGplayer Id: {row['TCGplayer Id']}. Skipping row."
                )
                continue
            except Exception as e:
                logger.error(f"Error: {e}")
                continue

        session.commit()


def convert_windows_path_to_wsl(path):
    try:
        path = path.replace("\\", "/")
        drive, path = path.split(":", 1)
        wsl_path = f"/mnt/{drive.lower()}{path}"
        if not os.path.exists(wsl_path):
            raise FileNotFoundError(f"The file does not exist at {wsl_path}")
        return wsl_path
    except Exception as e:
        raise ValueError(f"An error occurred while converting the path: {e}")

def generate_inventory(session: Session):
        boxes = session.query(Box).all()
        inventory = {"Inventory": [box.to_dict() for box in boxes]}

        with open('inventory.json', 'w') as f:
            json.dump(inventory, f, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload CSV to database")
    parser.add_argument("filepath", type=str, help="")
    inventory_status = InventoryStatus()
    args = parser.parse_args()

    engine = create_engine("sqlite:///mtg_inventory.db")
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(engine)
    session = Session()

    first_box = session.query(Box).first()
    if not first_box:
        new_box = Box()
        session.add(new_box)
        session.commit()

    with engine.connect() as connection:
        result = connection.execute(text("SELECT * FROM cards"))
        for row in result:
            print(row)

    generate_inventory(session)

    upload_from_csv(args.filepath, session, inventory_status)