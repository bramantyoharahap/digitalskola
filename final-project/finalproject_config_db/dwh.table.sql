create table dim_province(
	province_id int,
	province_name text
);

create table dim_district(
	district_id int,
	province_id int,
	district_name text
);

create table dim_case(
	id int,
	status_name text,
	status_detail text,
	status text
);

create table district_daily(
	id int,
	district_id text,
	case_id int,
	date date,
	total int
);

create table province_daily(
	id int,
	province_id text,
	case_id int,
	date date,
	total int
);