WITH enriched_orders AS (
    SELECT o.*, 
      CONCAT(f.name, ' ', f.species) AS order_contact_name, 
           REPLACE(
            REPLACE(
             CONCAT(
              LOWER(f.name), 
              '@', 
              LOWER(split_part(o.account_name, ' ', 1)), 
              '.com'
              ) 
            , ',', '')
            , '''', '') as order_contact_email, 
           ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY random()) AS rn
    FROM mode.dunder_mifflin_paper_sales o
    CROSS JOIN tutorial.animal_crossing_villagers f
)

SELECT 
  order_id,	
  purchased_at,	
  status,	
  cancelled_at,	
  returned_at,	
  product_id,	
  product_name,	
  price, 
  discount,	
  shipping_cost,	
  quantity,	
  business_size,	
  payment_cycle,	
  account_id,	
  account_name,	
  order_contact_name, 
  order_contact_email, 
  account_manager,
  days_to_close,
  shipping_mode,
  shipping_address,	
  shipping_city,
  shipping_state,	
  shipping_zip,	
  shipping_region,	
  shipping_latitude,	
  shipping_longitude,
  days_to_ship,	
  reviewed_at,	
  rating,	
  index,	
  review
FROM enriched_orders eo
WHERE eo.rn = 1
LIMIT 100000

Connection namePublic Warehouse

