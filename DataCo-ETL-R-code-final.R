###SQL Project Checkpoint 3

#author: "jz3195" & "yy3033" & "ne2295" & "jw3883"
#date: "2 Aug 2020"
library(DBI)
library(RPostgreSQL)
#read csv file
dataco = read.csv("~/Desktop/APAN 5310/final project/5310 dataset/DataCoSupplyChainDataset.csv")

str(dataco)
nrow(dataco)
head(dataco)
#--------------------------------Create Table Structure----------------------------------
require('RPostgreSQL')
#1. Load the PostgreSQL driver
drv <- dbDriver('PostgreSQL')

#2. Create a connection
con <- dbConnect(drv, dbname = 'dataco',
                 host = 'localhost', port = 5432,
                 user = 'postgres', password = '123')

#3. Pass the SQL statements that create all tables
stmt <- "
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS product_info;
DROP TABLE IF EXISTS product_category;
DROP TABLE IF EXISTS department_info;
DROP TABLE IF EXISTS shipping_info;
DROP TABLE IF EXISTS shipping_mode;
DROP TABLE IF EXISTS order_info;
DROP TABLE IF EXISTS customer_info;
DROP TABLE IF EXISTS payment_info;
DROP TABLE IF EXISTS store_info;
DROP TABLE IF EXISTS city_state;
DROP TABLE IF EXISTS state_country;
DROP TABLE IF EXISTS country_info;
DROP TABLE IF EXISTS region_market;
DROP TABLE IF EXISTS market_info;


--TABLE-1
    CREATE TABLE market_info(
	  market_id SERIAL,
    market_name VARCHAR(100),
  	PRIMARY KEY (market_id),
  	CHECK (market_name IN ('Pacific Asia','USCA','Africa','Europe','LATAM'))
);
--TABLE-2
CREATE TABLE region_market(
  	region_id SERIAL,
  	region_name VARCHAR(100),
  	market_id SERIAL,
  	PRIMARY KEY (region_id),
  	FOREIGN KEY (market_id) REFERENCES market_info(market_id)
  	ON UPDATE CASCADE
	  ON DELETE CASCADE
);
--TABLE-3
CREATE TABLE country_info(
  	country_id SERIAL,
  	country_name VARCHAR(100),
  	PRIMARY KEY (country_id)
);

--TABLE-4
CREATE TABLE state_country(
  	state_id SERIAL,
  	state_name VARCHAR(100),
  	country_id SERIAL,
  	PRIMARY KEY (state_id),
  	FOREIGN KEY (country_id) REFERENCES country_info(country_id)
  	ON UPDATE CASCADE
  	ON DELETE CASCADE
);

--TABLE-5
CREATE TABLE city_state(
	  city_id SERIAL,
	  city_name VARCHAR(100) NOT NULL,
	  state_id SERIAL,
	  PRIMARY KEY (city_id),
    FOREIGN KEY (state_id) REFERENCES state_country(state_id)
    ON UPDATE CASCADE
	  ON DELETE CASCADE
);

--TABLE-6
CREATE TABLE store_info(
  	store_id SERIAL,
  	customer_street VARCHAR(225),
  	PRIMARY KEY (store_id)
);

--TABLE-7
CREATE TABLE payment_info(
   payment_type_id SERIAL,
  	payment_type_name VARCHAR(100),
  	PRIMARY KEY (payment_type_id)
);

--TABLE-8
CREATE TABLE customer_info(
  	customer_id SERIAL,
  	customer_fname VARCHAR(30) NOT NULL,
  	customer_lname VARCHAR(30) NOT NULL,
  	customer_email VARCHAR(225) NOT NULL,
  	customer_password VARCHAR(30),
	  customer_segment VARCHAR(50) NOT NULL,
	  latitude NUMERIC,
	  longitude NUMERIC,
  	customer_zipcode INT,
  	customer_city_id INT,
	  PRIMARY KEY (customer_id),
	  FOREIGN KEY (customer_city_id) REFERENCES city_state(city_id)
    ON UPDATE CASCADE
	  ON DELETE CASCADE,
    CHECK (customer_segment IN ('Consumer','Corporate','Home Office'))
);

--TABLE-9
CREATE TABLE order_info(
  	order_id SERIAL,
  	customer_id SERIAL,
  	store_id SERIAL,
  	order_date TIMESTAMP,
  	order_status VARCHAR(50),
  	payment_type_id SERIAL,
    order_zipcode INT,
    order_city_id SERIAL,
	  region_id SERIAL,
 	  PRIMARY KEY(order_id),
 	  FOREIGN KEY (customer_id) REFERENCES customer_info(customer_id)
	  ON UPDATE CASCADE
	  ON DELETE CASCADE,
  	FOREIGN KEY (store_id) REFERENCES store_info(store_id)
  	ON UPDATE CASCADE
  	ON DELETE CASCADE,
  	FOREIGN KEY (payment_type_id) REFERENCES payment_info(payment_type_id)
  	ON UPDATE CASCADE
    ON DELETE CASCADE,
    FOREIGN KEY (order_city_id) REFERENCES city_state(city_id)
  	ON UPDATE CASCADE
    ON DELETE CASCADE,
	FOREIGN KEY (region_id) REFERENCES region_market(region_id)
  	ON UPDATE CASCADE
    ON DELETE CASCADE
);

--TABLE-10
CREATE TABLE shipping_mode(
    shipping_mode_id SERIAL,
	  shipping_mode_name VARCHAR(100),
	  PRIMARY KEY (shipping_mode_id)
);


--TABLE-11
CREATE TABLE shipping_info(
    shipping_id SERIAL,
  	order_id SERIAL,
    ship_day_real INT,
    ship_day_schedule INT,
 	  delivery_status VARCHAR(50),
 	  shipping_date TIMESTAMP,
 	  shipping_mode_id SERIAL,
  	late_delivery_risk boolean,
  	PRIMARY KEY(shipping_id),
  	FOREIGN KEY (order_id) REFERENCES order_info(order_id)
  	ON UPDATE CASCADE
  	ON DELETE CASCADE,
  	FOREIGN KEY (shipping_mode_id) REFERENCES shipping_mode(shipping_mode_id)
  	ON UPDATE CASCADE
    ON DELETE CASCADE,
  	CHECK (delivery_status IN ('Advance shipping', 'Late delivery', 'Shipping canceled', 'Shipping on time'))
);

--TABLE-12
CREATE TABLE department_info(
  	department_id SERIAL,
  	department_name VARCHAR(100),
  	PRIMARY KEY (department_id)
);

--TABLE-13
CREATE TABLE product_category(
  	product_category_id SERIAL,
  	product_category_name VARCHAR(50),
  	department_id SERIAL NOT NULL,
  	PRIMARY KEY(product_category_id),
  	FOREIGN KEY (department_id) REFERENCES department_info(department_id)
  	ON UPDATE CASCADE
    ON DELETE CASCADE
);

--TABLE-14
CREATE TABLE product_info(
  	product_id SERIAL,
  	product_price DECIMAL(10,2) NOT NULL,
  	product_description TEXT,
  	product_name VARCHAR(50) NOT NULL,
 	  product_status INT,
  	product_category_id SERIAL,
 	  PRIMARY KEY(product_id),
  	CHECK (product_status IN (0, 1)),
  	FOREIGN KEY (product_category_id) REFERENCES product_category(product_category_id)
  	ON UPDATE CASCADE
    ON DELETE CASCADE
);

--TABLE-15
CREATE TABLE order_items(
  	order_item_id SERIAL,
  	order_id SERIAL,
  	product_id SERIAL,
  	order_item_fullsale DECIMAL(20,8),
  	order_item_realsale DECIMAL(20,8),
    order_item_discount_value DECIMAL(20,8),
  	order_item_discount_rate DECIMAL(20,8) NOT NULL,
  	order_item_quantity INT,
  	order_item_profit DECIMAL(20,8),
  	order_item_profit_ratio DECIMAL(20,8),
  	PRIMARY KEY(order_item_id),
  	FOREIGN KEY (order_id) REFERENCES order_info(order_id)
  	ON UPDATE CASCADE
    ON DELETE CASCADE,
  	FOREIGN KEY (product_id) REFERENCES product_info(product_id)
  	ON UPDATE CASCADE
  	ON DELETE CASCADE
);
"
#4. Execute the statement to create tables
dbGetQuery(con, stmt)

#5. Rename colnames
colnames(dataco)
names(dataco)=c('payment_type_name','ship_day_real','ship_day_schedule','Benefit.per.order',
                'Sales.per.customer','delivery_status','late_delivery_risk','product_category_id',
                'product_category_name','customer_city','customer_country','customer_email',
                'customer_fname','customer_id','customer_lname','customer_password',
                'customer_segment','customer_state','customer_street','customer_zipcode',
                'department_id','department_name','latitude','longitude',
                'market_name','order_city','order_country','Order.Customer.Id',
                'order_date','order_id','Order.Item.Cardprod.Id','order_item_discount_value',
                'order_item_discount_rate','order_item_id','product_price','order_item_profit_ratio',
                'order_item_quantity','order_item_fullsale','order_item_realsale','order_item_profit',
                'order_region','order_state','order_status','order_zipcode',
                'product_id','Product.Category.Id','product_description','Product.Image',
                'product_name','product.price','product_status','shipping_date',
                'shipping_mode_name')
dataco=dataco[,-48]
nrow(dataco)
head(dataco)
#--------------------------------Create Tables----------------------------------
#table1: market_info
market_info <- unique(dataco[c('market_name')])
market_info$market_id <- 1:nrow(market_info)
head(market_info)
nrow(market_info)
market_id_list <- sapply(dataco$market_name, function(x) market_info$market_id[market_info$market_name == x])
dataco$market_id <- market_id_list
colnames(dataco)
head(dataco)
dbWriteTable(con, name="market_info", value=market_info, row.names=FALSE, append=TRUE)

#table2: region_market
region_market <- unique(dataco[c('order_region','market_id')])
names(region_market)<-c('region_name','market_id')
region_market$region_id <- 1:nrow(region_market)
nrow(region_market)
region_id_list <- sapply(dataco$order_region, function(x) region_market$region_id[region_market$region_name == x])
dataco$region_id <- region_id_list
colnames(dataco)
nrow(dataco)
head(dataco)
dbWriteTable(con, name="region_market", value=region_market, row.names=FALSE, append=TRUE)

#table3: country_info
country1=dataco['customer_country']
country2=dataco['order_country']
names(country1)=c('country_name')
names(country2)=c('country_name')
country_info=rbind(country1,country2)
country_info <- unique(country_info)
country_info$country_id <- 1:nrow(country_info)
head(country_info)
nrow(country_info)
dbWriteTable(con, name="country_info", value=country_info, row.names=FALSE, append=TRUE)

names(country_info)=c('order_country','order_country_id')
dataco=merge(dataco ,country_info ,all.x=TRUE,sort=TRUE) 
colnames(dataco)
head(dataco)

names(country_info)=c('customer_country','customer_country_id')
dataco=merge(dataco ,country_info ,all.x=TRUE,sort=TRUE) 
colnames(dataco)
head(dataco)

#table4:state_country
state1=dataco[c('customer_state','customer_country_id')]
state2=dataco[c('order_state','order_country_id')]
names(state1)=c('state_name','country_id')
names(state2)=c('state_name','country_id')
state_country=rbind(state1,state2)
state_country <- unique(state_country)
state_country$state_id <- 1:nrow(state_country)
head(state_country)
nrow(state_country)
dbWriteTable(con, name="state_country", value=state_country, row.names=FALSE, append=TRUE)

names(state_country)=c('order_state','order_country_id','order_state_id')
dataco=merge(dataco ,state_country ,all.x=TRUE,sort=TRUE) 
colnames(dataco)
head(dataco)

names(state_country)=c('customer_state','customer_country_id','customer_state_id')
dataco=merge(dataco ,state_country ,all.x=TRUE,sort=TRUE) 
colnames(dataco)
head(dataco)

#table5: city_state
city1=dataco[c('customer_city','customer_state_id')]
city2=dataco[c('order_city','order_state_id')]
names(city1)=c('city_name','state_id')
names(city2)=c('city_name','state_id')
city_state=rbind(city1,city2)
city_state <- unique(city_state)
city_state$city_id <- 1:nrow(city_state)
head(city_state)
nrow(city_state)
dbWriteTable(con, name="city_state", value=city_state, row.names=FALSE, append=TRUE)

names(city_state)=c('order_city','order_state_id','order_city_id')
dataco=merge(dataco ,city_state ,all.x=TRUE,sort=TRUE) 
colnames(dataco)
head(dataco)

names(city_state)=c('customer_city','customer_state_id','customer_city_id')
dataco=merge(dataco ,city_state ,all.x=TRUE,sort=TRUE) 
colnames(dataco)
head(dataco)

#table6: store_info
store_info=unique(dataco[c('customer_street')])
nrow(store_info)
store_info$store_id <- 1:nrow(store_info)
head(store_info)
nrow(store_info)
dbWriteTable(con, name="store_info", value=store_info, row.names=FALSE, append=TRUE)

store_id_list <- sapply(dataco$customer_street, function(x) store_info$store_id[store_info$customer_street == x])
dataco$store_id <- store_id_list
head(dataco)

#table7: payment_info
payment_info=unique(dataco[c('payment_type_name')])
payment_info$payment_type_id <- 1:nrow(payment_info)
head(payment_info)
nrow(payment_info)
dbWriteTable(con, name="payment_info", value=payment_info, row.names=FALSE, append=TRUE)

payment_id_list <- sapply(dataco$payment_type, function(x) payment_info$payment_type_id[payment_info$payment_type_name == x])
dataco$payment_type_id <- payment_id_list

#table8: customer_info
customer_info=unique(dataco[c('customer_id','customer_fname','customer_lname',
                              'customer_email','customer_password','customer_segment',
                              'latitude','longitude','customer_zipcode','customer_city_id')])
nrow(customer_info)
dbWriteTable(con, name="customer_info", value=customer_info, row.names=FALSE, append=TRUE)

#table9: order_info
order_info=unique(dataco[c('order_id','customer_id','store_id','order_date',
                           'order_status','payment_type_id','order_zipcode','order_city_id','region_id')])
nrow(order_info)
order_info$order_date = as.POSIXct(order_info$order_date,format="%m/%d/%Y %H:%M",tz = "UTC")
dbWriteTable(con, name="order_info", value=order_info, row.names=FALSE, append=TRUE)
colnames(dataco)
head(order_info)

#table10: shipping_mode
shipping_mode <- unique(dataco[c('shipping_mode_name')])
shipping_mode$shipping_mode_id <- 1:nrow(shipping_mode)
head(shipping_mode)
nrow(shipping_mode)
shipping_mode_id_list <- sapply(dataco$shipping_mode_name, function(x) shipping_mode$shipping_mode_id[shipping_mode$shipping_mode_name == x])
dataco$shipping_mode_id <- shipping_mode_id_list
dbWriteTable(con, name="shipping_mode", value=shipping_mode, row.names=FALSE, append=TRUE)

#table11: shipping_info
shipping_info <- unique(dataco[c('order_id','ship_day_real','ship_day_schedule',
                                 'delivery_status','shipping_date','shipping_mode_id',
                                 'late_delivery_risk')])
shipping_info$shipping_id <- 1:nrow(shipping_info)
shipping_info$shipping_date = as.POSIXct(shipping_info$shipping_date,format="%m/%d/%Y %H:%M",tz = "UTC")
head(shipping_info)
nrow(shipping_info)
dataco <- merge(dataco,shipping_info,all.x = TRUE,sort = TRUE)
nrow(dataco)
colnames(dataco)
dbWriteTable(con, name="shipping_info", value=shipping_info, row.names=FALSE, append=TRUE)

#table12: department_info
department_info=unique(dataco[c('department_id','department_name')])
nrow(department_info)
dbWriteTable(con, name="department_info", value=department_info, row.names=FALSE, append=TRUE)

#table13: product_category
product_category=unique(dataco[c('product_category_id','product_category_name','department_id')])
nrow(product_category)
dbWriteTable(con, name="product_category", value=product_category, row.names=FALSE, append=TRUE)

#table14: product_info
product_info=unique(dataco[c('product_id','product_price','product_description',
                             'product_name','product_status','product_category_id')])
nrow(product_info)
dbWriteTable(con, name="product_info", value=product_info, row.names=FALSE, append=TRUE)

#table15: order_items
order_items=unique(dataco[c('order_item_id','order_id','product_id',
                             'order_item_realsale','order_item_fullsale','order_item_discount_value',
                            'order_item_discount_rate','order_item_quantity','order_item_profit',
                            'order_item_profit_ratio')])
nrow(order_items)
dbWriteTable(con, name="order_items", value=order_items, row.names=FALSE, append=TRUE)

#end