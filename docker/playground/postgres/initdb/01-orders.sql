-- Sample table for the playground. Runs once, as user demo, in database demo.
CREATE TABLE orders (
    order_id    bigint PRIMARY KEY,
    customer    text,
    product     text,
    quantity    int,
    unit_price  numeric(12,2),
    status      text,
    created_at  timestamp DEFAULT now()
) USING paimon_heap;

INSERT INTO orders (order_id, customer, product, quantity, unit_price, status) VALUES
    (1, 'Alice',   'Laptop',     1, 1299.99, 'shipped'),
    (2, 'Bob',     'Keyboard',   2,   79.50, 'pending'),
    (3, 'Charlie', 'Monitor',    1,  349.00, 'delivered'),
    (4, 'Diana',   'Headphones', 3,   59.99, 'shipped'),
    (5, 'Eve',     'Webcam',     1,   89.00, 'pending');
