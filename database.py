from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    create_engine
)
from sqlalchemy.orm import (
    declarative_base,
    relationship,
    sessionmaker
)

# Initialize SQLite Database Engine
engine = create_engine(
    "sqlite:///warehouse.db",
    echo=False
)

Base = declarative_base()


# ==========================
# ORDERS
# ==========================

class Order(Base):
    __tablename__ = "orders"

    id = Column(
        Integer,
        primary_key=True
    )

    # DEAR/Cin7 Sale ID (the GUID). Kept unique so re-fetching the same sale
    # updates the existing row instead of duplicating it.
    external_id = Column(
        String,
        unique=True,
        index=True
    )

    # Human-facing sale order number, e.g. SO-00100.
    order_number = Column(
        String,
        index=True
    )

    customer = Column(String)
    email = Column(String)
    phone = Column(String)
    address = Column(String)

    # DEAR Sale ID again, stored separately so the fulfilment push has a
    # dedicated field even if external_id is ever repurposed.
    sale_id = Column(String)

    status = Column(
        String,
        default="OPEN"
    )

    created_at = Column(
        DateTime,
        default=datetime.now
    )

    items = relationship(
        "OrderItem",
        back_populates="order"
    )

    shipments = relationship(
        "Shipment",
        back_populates="order"
    )


# ==========================
# ORDER ITEMS
# ==========================

class OrderItem(Base):
    __tablename__ = "order_items"

    id = Column(
        Integer,
        primary_key=True
    )

    order_id = Column(
        Integer,
        ForeignKey("orders.id"),
        index=True
    )

    order = relationship(
        "Order",
        back_populates="items"
    )

    # Stable per-line key. For DEAR this holds the SKU, which is what the app
    # matches on when recording a shipment. Matching on title alone breaks
    # when two lines share a product title.
    fo_line_item_id = Column(
        String,
        index=True
    )

    sku = Column(String)
    title = Column(String)
    variant = Column(String)
    unit_weight = Column(Float)
    quantity = Column(Integer)

    dispatched_quantity = Column(
        Integer,
        default=0
    )


# ==========================
# SHIPMENTS
# ==========================

class Shipment(Base):
    __tablename__ = "shipments"

    id = Column(
        Integer,
        primary_key=True
    )

    order_id = Column(
        Integer,
        ForeignKey("orders.id"),
        index=True
    )

    order = relationship(
        "Order",
        back_populates="shipments"
    )

    # Left blank in the CSV flow (no AusPost API shipment id). Kept for
    # compatibility and possible future use.
    shipment_id = Column(String)

    tracking_number = Column(String)
    shipping_service = Column(String)

    # Blank in the CSV flow - labels are printed from the AusPost portal.
    label_path = Column(String)
    manifest_id = Column(String)

    shipped_date = Column(
        DateTime,
        default=datetime.now
    )


# ==========================
# ERROR LOG
# ==========================

class ErrorLog(Base):
    __tablename__ = "error_logs"

    id = Column(
        Integer,
        primary_key=True
    )

    module = Column(String)
    message = Column(String)

    created = Column(
        DateTime,
        default=datetime.now
    )


# Create Database Tables if they do not exist
Base.metadata.create_all(engine)

Session = sessionmaker(
    bind=engine
)


# ==========================
# SAVE DEAR SALE
# ==========================

def save_order(order):
    """
    Create the order + its line items if new. If the order already exists,
    refresh its line items instead of skipping - packed quantities can change
    between fetches, and skipping would leave stale data in place.

    Expects the app's normalised dict:
      Order ID, Order Name, Customer, Email, Phone, Raw Address,
      Fulfillment Order ID (the DEAR Sale ID), Line Items[].
    """
    db = Session()

    try:
        external_id = order["Order ID"]

        existing = db.query(Order).filter_by(
            external_id=external_id
        ).first()

        if existing:
            existing.order_number = order["Order Name"]
            existing.customer = order.get("Customer", "")
            existing.email = order.get("Email", "")
            existing.phone = order.get("Phone", "")
            existing.address = str(order.get("Raw Address", {}))
            existing.sale_id = order.get("Fulfillment Order ID", external_id)
            _upsert_line_items(db, existing, order["Line Items"])
            db.commit()
            return

        new_order = Order(
            external_id=external_id,
            order_number=order["Order Name"],
            customer=order.get("Customer", ""),
            email=order.get("Email", ""),
            phone=order.get("Phone", ""),
            address=str(order.get("Raw Address", {})),
            sale_id=order.get("Fulfillment Order ID", external_id)
        )

        db.add(new_order)
        db.commit()
        db.refresh(new_order)

        _upsert_line_items(db, new_order, order["Line Items"])
        db.commit()

    except Exception as e:
        db.rollback()
        log_error("save_order", str(e))
        raise
    finally:
        db.close()


def _upsert_line_items(db, order_obj, line_items):
    """
    Insert new line items, or update the quantity on existing ones, matched by
    fo_line_item_id (the SKU) rather than title (not unique within an order).
    """
    for item in line_items:
        fo_id = item.get("fo_line_item_id") or item.get("sku")

        existing_item = None
        if fo_id:
            existing_item = db.query(OrderItem).filter_by(
                order_id=order_obj.id,
                fo_line_item_id=fo_id
            ).first()

        if existing_item:
            existing_item.quantity = item["remaining_qty"]
            existing_item.unit_weight = item.get("unit_weight_kg", 0)
        else:
            db.add(OrderItem(
                order_id=order_obj.id,
                fo_line_item_id=fo_id,
                sku=item.get("sku", ""),
                title=item["title"],
                variant=item.get("variant", ""),
                unit_weight=item.get("unit_weight_kg", 0),
                quantity=item["remaining_qty"]
            ))


# ==========================
# SAVE SHIPMENT
# ==========================

def save_shipment(
    order_number,
    shipment_id,
    tracking,
    service,
    label,
    manifest,
    dispatched_items
):
    db = Session()

    try:
        order = db.query(Order).filter_by(
            order_number=order_number
        ).first()

        if not order:
            log_error("save_shipment", f"No matching order found for order_number={order_number}")
            return

        shipment = Shipment(
            order_id=order.id,
            shipment_id=shipment_id,
            tracking_number=tracking,
            shipping_service=service,
            label_path=label,
            manifest_id=manifest
        )

        db.add(shipment)

        # Update dispatched quantities, matched by the stable fo_line_item_id.
        for shipped in dispatched_items:
            fo_id = shipped.get("fo_line_item_id")

            item = None
            if fo_id:
                item = db.query(OrderItem).filter_by(
                    order_id=order.id,
                    fo_line_item_id=fo_id
                ).first()

            if not item:
                item = db.query(OrderItem).filter_by(
                    order_id=order.id,
                    title=shipped["title"]
                ).first()

            if item:
                item.dispatched_quantity += shipped["dispatch_qty"]
            else:
                log_error(
                    "save_shipment",
                    f"No matching line item for order_number={order_number}, "
                    f"fo_line_item_id={fo_id}, title={shipped.get('title')}"
                )

        # Evaluate order fulfilment status.
        completed = True
        partial = False
        for item in order.items:
            if item.dispatched_quantity < item.quantity:
                completed = False
                if item.dispatched_quantity > 0:
                    partial = True

        if completed:
            order.status = "SHIPPED"
        elif partial:
            order.status = "PARTIALLY_SHIPPED"

        db.commit()

    except Exception as e:
        db.rollback()
        log_error("save_shipment", str(e))
        raise
    finally:
        db.close()


# ==========================
# ERROR LOGGER
# ==========================

def log_error(module, message):
    db = Session()

    try:
        error = ErrorLog(
            module=module,
            message=message
        )
        db.add(error)
        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()