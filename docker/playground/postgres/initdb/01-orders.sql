-- Sample table for the playground. Runs once, as user demo, in database demo.
CREATE TABLE orders (
    order_id    bigint PRIMARY KEY,
    customer    text,
    product     text,
    quantity    int,
    unit_price  numeric(12,2),
    status      text,
    created_at  timestamp
) USING paimon_heap;

INSERT INTO orders (order_id, customer, product, quantity, unit_price, status, created_at) VALUES
    (1, 'Alice',   'Laptop',     1, 1299.99, 'shipped', now()),
    (2, 'Bob',     'Keyboard',   2,   79.50, 'pending', now()),
    (3, 'Charlie', 'Monitor',    1,  349.00, 'delivered', now()),
    (4, 'Diana',   'Headphones', 3,   59.99, 'shipped', now()),
    (5, 'Eve',     'Webcam',     1,   89.00, 'pending', now());
