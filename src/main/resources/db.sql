CREATE TABLE mammal (
                         id serial primary key not null ,
                         name varchar(30),
                         genus varchar(30),
                         age int,
                         is_male bool,
                         is_up_right bool,
                         is_season_hibernation bool
);

CREATE TABLE bird (
                         id serial primary key not null ,
                         name varchar(30),
                         genus varchar(30),
                         age int,
                         is_male bool,
                         is_flying bool,
                         is_migrating bool
);