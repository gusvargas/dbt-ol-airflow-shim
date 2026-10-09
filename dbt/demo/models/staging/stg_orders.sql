select *
from (
    values
        (1, 'A-1001', 'widget', 10.00),
        (2, 'A-1002', 'gadget', 15.00),
        (3, 'A-1003', 'gizmo', 20.00)
) as t(order_id, order_number, product, amount)
