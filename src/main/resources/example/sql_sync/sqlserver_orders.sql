CREATE TABLE user_
(
    id   BIGINT primary key,
    name varchar(64)
) WITH (
      'connector' = 'kafka',
      'topic' = 'local.demo.user',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'localDemoUserConsumer',
      'format' = 'debezium-json',
      'scan.startup.mode' = 'earliest-offset'
      )

----
CREATE TABLE orders
(
    order_no       varchar(64) primary key,
    create_user_id bigint,
    removed        boolean
) WITH (
      'connector' = 'kafka',
      'topic' = 'local.demo.orders',
      'properties.bootstrap.servers' = 'localhost:9092',
      'properties.group.id' = 'localDemoOrdersConsumer',
      'format' = 'debezium-json',
      'scan.startup.mode' = 'earliest-offset'
      )

----
CREATE TABLE sink
(
    order_no  varchar(64) primary key,
    user_id   bigint,
    user_name varchar(64)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:jtds:sqlserver://localhost:1433/demo',
    'username' = 'root',
    'password' = 'root',
    'table-name' = 'orders_result'
      )
----
insert into sink
select o.order_no, u.id user_id, u.name user_name
from orders o
left join user_ u on u.id = o.create_user_id
where o.removed is null
   or o.removed = false