-- Database: Real-Estate-Management

-- DROP DATABASE IF EXISTS "Real-Estate-Management";

CREATE DATABASE "Real-Estate-Management"
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_United States.1252'
    LC_CTYPE = 'English_United States.1252'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

-- Create Address Table
CREATE TABLE Address (
    address_id SERIAL PRIMARY KEY,
    street_address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    zip_code VARCHAR(20),
    latitude NUMERIC,
    longitude NUMERIC
);

-- Create Client Table
CREATE TABLE Client (
    client_id SERIAL PRIMARY KEY,
    client_name VARCHAR(255),
    client_gender VARCHAR(20),
    client_phone VARCHAR(20),
    client_email VARCHAR(255),
    client_dob DATE,
    address_id INT REFERENCES Address(address_id)
);

-- Create Agent Table
CREATE TABLE Agent (
    agent_id SERIAL PRIMARY KEY,
    agent_name VARCHAR(255),
    agent_gender VARCHAR(20),
    agent_phone VARCHAR(20),
    agent_email VARCHAR(255),
    agent_dob DATE,
    address_id INT REFERENCES Address(address_id),
    hire_date DATE,
    title VARCHAR(100)
);

-- Create Owner Table
CREATE TABLE Owner (
    owner_id SERIAL PRIMARY KEY,
    owner_name VARCHAR(255),
    owner_gender VARCHAR(20),
    owner_phone VARCHAR(20),
    owner_email VARCHAR(255),
    owner_dob DATE,
    address_id INT REFERENCES Address(address_id)
);

-- Create Features Table
CREATE TABLE Features (
    feature_id SERIAL PRIMARY KEY,
    lot_area_sqft INT,
    no_bedrooms INT,
    no_bathrooms INT,
    no_kitchens INT,
    no_floors INT,
    parking_area_sqft INT,
    year_built INT,
    condition_rating INT
);

-- Create Property Table
CREATE TABLE Property (
    property_id SERIAL PRIMARY KEY,
    address_id INT UNIQUE REFERENCES Address(address_id),
    owner_id INT REFERENCES Owner(owner_id),
    agent_id INT REFERENCES Agent(agent_id),
    feature_id INT REFERENCES Features(feature_id),
    listing_date DATE,
    listing_type VARCHAR(50),
    asking_amount NUMERIC
);

-- Maintenance Table
CREATE TABLE Maintenance (
    maintenance_id SERIAL PRIMARY KEY,
    property_id INT REFERENCES Property(property_id),
    maintenance_date DATE,
    maintenance_type VARCHAR(100),
    cost NUMERIC,
    description TEXT
);

-- Create Visit Table
CREATE TABLE Visit (
    visit_id SERIAL PRIMARY KEY,
    property_id INT REFERENCES Property(property_id),
    visit_date DATE,
    client_id INT REFERENCES Client(client_id),
    description TEXT
);

-- Create Commission Table
CREATE TABLE Commission (
    commission_id SERIAL PRIMARY KEY,
    commission_rate NUMERIC,
    commission_amount NUMERIC,
    payment_method VARCHAR(50),
    payment_date DATE
);

-- Create Sale Table
CREATE TABLE Sale (
    sale_id SERIAL PRIMARY KEY,
    property_id INT REFERENCES Property(property_id),
    sale_date DATE,
    client_id INT REFERENCES Client(client_id),
    commission_id INT REFERENCES Commission(commission_id),
    sale_amount NUMERIC
);

-- Create Contract Table
CREATE TABLE Contract (
    contract_id SERIAL PRIMARY KEY,
    contract_terms TEXT
);

-- Create Rent Table
CREATE TABLE Rent (
    rent_id SERIAL PRIMARY KEY,
    property_id INT REFERENCES Property(property_id),
    agreement_date DATE,
    rent_start_date DATE,
    rent_end_date DATE,
    rent_amount NUMERIC,
    client_id INT REFERENCES Client(client_id),
    commission_id INT REFERENCES Commission(commission_id),
    contract_id INT REFERENCES Contract(contract_id)
);

-- Create Admin Table
CREATE TABLE Admin (
    admin_id SERIAL PRIMARY KEY,
    admin_name VARCHAR(255),
    username VARCHAR(100) UNIQUE,
    password VARCHAR(255)
);

-- ===========================
-- Address
-- ===========================
INSERT INTO Address (street_address, city, state, zip_code, latitude, longitude) VALUES
('123 Maple Street', 'New York', 'NY', '10001', 40.7128, -74.0060),
('456 Oak Avenue', 'Los Angeles', 'CA', '90001', 34.0522, -118.2437),
('789 Pine Road', 'Chicago', 'IL', '60601', 41.8781, -87.6298),
('321 Cedar Blvd', 'Houston', 'TX', '77001', 29.7604, -95.3698),
('654 Birch Lane', 'Miami', 'FL', '33101', 25.7617, -80.1918);

-- ===========================
-- Client
-- ===========================
INSERT INTO Client (client_name, client_gender, client_phone, client_email, client_dob, address_id) VALUES
('John Doe', 'Male', '1234567890', 'john@example.com', '2/21/1997', 1),
('Jane Smith', 'Female', '9876543210', 'jane@example.com', '5/12/1995', 2),
('Robert Brown', 'Male', '5551234567', 'robert@example.com', '8/30/1988', 3),
('Emily Davis', 'Female', '4449871234', 'emily@example.com', '9/15/1992', 4),
('Michael Johnson', 'Male', '3335557777', 'michael@example.com', '12/1/1985', 5);

-- ===========================
-- Agent
-- ===========================
INSERT INTO Agent (agent_name, agent_gender, agent_phone, agent_email, agent_dob, address_id, hire_date, title) VALUES
('Sarah Lee', 'Female', '1112223333', 'sarah.lee@agency.com', '3/10/1989', 1, '6/1/2015', 'Senior Agent'),
('David Miller', 'Male', '2223334444', 'david.miller@agency.com', '11/5/1986', 2, '4/20/2016', 'Junior Agent'),
('Sophia Taylor', 'Female', '3334445555', 'sophia.taylor@agency.com', '7/19/1990', 3, '2/10/2018', 'Listing Agent');

-- ===========================
-- Owner
-- ===========================
INSERT INTO Owner (owner_name, owner_gender, owner_phone, owner_email, owner_dob, address_id) VALUES
('William Clark', 'Male', '5556667777', 'william@owner.com', '10/22/1975', 1),
('Olivia Lewis', 'Female', '8889990000', 'olivia@owner.com', '4/5/1980', 2),
('Henry Moore', 'Male', '9990001111', 'henry@owner.com', '1/15/1978', 3);

-- ===========================
-- Features
-- ===========================
INSERT INTO Features (lot_area_sqft, no_bedrooms, no_bathrooms, no_kitchens, no_floors, parking_area_sqft, year_built, condition_rating) VALUES
(2500, 3, 2, 1, 2, 300, 2015, 8),
(3200, 4, 3, 1, 2, 400, 2018, 9),
(1800, 2, 1, 1, 1, 200, 2012, 7);

-- ===========================
-- Property
-- ===========================
INSERT INTO Property (address_id, owner_id, agent_id, feature_id, listing_date, listing_type, asking_amount) VALUES
(1, 1, 1, 1, '5/15/2023', 'Sale', 450000),
(2, 2, 2, 2, '7/20/2023', 'Rent', 2500),
(3, 3, 3, 3, '8/10/2023', 'Sale', 380000);

-- ===========================
-- Maintenance
-- ===========================
INSERT INTO Maintenance (property_id, maintenance_date, maintenance_type, cost, description) VALUES
(1, '6/1/2023', 'Plumbing', 500, 'Fixed kitchen sink'),
(2, '8/5/2023', 'Painting', 700, 'Repainted living room'),
(3, '9/12/2023', 'Roof Repair', 1200, 'Fixed minor roof leaks');

-- ===========================
-- Visit
-- ===========================
INSERT INTO Visit (property_id, visit_date, client_id, description) VALUES
(1, '6/15/2023', 1, 'Client interested in buying'),
(2, '8/10/2023', 2, 'Client visited for rent inquiry'),
(3, '9/5/2023', 3, 'Client considering purchase');

-- ===========================
-- Commission
-- ===========================
INSERT INTO Commission (commission_rate, commission_amount, payment_method, payment_date) VALUES
(0.05, 22500, 'Bank Transfer', '6/25/2023'),
(0.03, 75, 'Cash', '8/20/2023'),
(0.04, 15200, 'Online', '9/25/2023');

-- ===========================
-- Sale
-- ===========================
INSERT INTO Sale (property_id, sale_date, client_id, commission_id, sale_amount) VALUES
(1, '6/20/2023', 1, 1, 450000),
(3, '9/20/2023', 3, 3, 380000);

-- ===========================
-- Contract
-- ===========================
INSERT INTO Contract (contract_terms) VALUES
('12-month rental contract. Security deposit equal to one month rent.'),
('6-month lease with renewal option.');

-- ===========================
-- Rent
-- ===========================
INSERT INTO Rent (property_id, agreement_date, rent_start_date, rent_end_date, rent_amount, client_id, commission_id, contract_id) VALUES
(2, '8/15/2023', '9/1/2023', '8/31/2024', 2500, 2, 2, 1);

-- ===========================
-- Admin
-- ===========================
INSERT INTO Admin (admin_name, username, password) VALUES
('Admin One', 'admin1', 'asifa123'),
('Admin Two', 'admin2', 'secure456');

select * from Address;
select * from Client;
select * from Agent;
select * from Owner;
select * from Features;
select * from Property;
select * from Maintenance;
select * from Visit;
select * from Commission;
select * from Sale;
select * from Rent;
select * from Contract;
select * from Admin;
select * from dim_propertydetails;
select * from dim_agent;