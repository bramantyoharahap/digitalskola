create table dim_province(
	province_id int,
	province_name varchar(100)
);

create table dim_district(
	district_id int,
	province_id int,
	province_name varchar(100)
);

create table dim_case(
	id int,
	status_name varchar(100),
	status_detail varchar(100),
	status varchar(100)
);

create table district_daily(
	id int,
	district_id int,
	case_id int,
	date date,
	total int
);

create table province_daily(
	id int,
	province_id int,
	case_id int,
	date date,
	total int
);