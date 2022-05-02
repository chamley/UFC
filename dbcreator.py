from dbhelper import DBHelper


def main():
    """The purpose of this script is to fully fledge out version 1.0 of our database"""
    db = DBHelper()
    # create_Date_dim(db)
    # create_Fighter_dim(db)
    # create_Round_dim(db)
    db.closeDB()


def create_Round_fact() -> None:
    pass


def populate_Date_dim() -> None:
    pass


def create_Fight_dim() -> None:
    query = """
            create table Fight_dim (
                fight_key int primary key
                ,fight_date_key int not null
                ,red_fighter_key int not null
                ,blue_fighter_key int not null
                ,location varchar(20)
                ,referee varchar(20)
                ,weight_class varchar(20)
                ,winner_key int
                ,method varchar(20)
                ,round int
                ,time time

            )
    """


def create_Round_dim(db: DBHelper) -> None:
    query = """
        create table Round_dim (
            round_key int primary key
            ,fight_key int not null
            ,round_number smallint not null
            );"""
    db.getCursor().execute(query)
    db.getConn().commit()


def create_Fighter_dim(db: DBHelper) -> None:
    query = """
        create TABLE Fighter_dim (
            fighter_key int primary key
            ,date_of_birth_key int
            ,first_name varchar(20) not null
            ,last_name varchar(20) not null
            ,height float
            ,reach float
            ,stance varchar(20)
        );
    """
    db.getCursor().execute(query)
    db.getConn().commit()


def create_Date_dim(db: DBHelper) -> None:
    query = """CREATE TABLE Date_dim (
                    date_key int primary key
                    ,year varchar(2) not null
                    ,month varchar(2) not null
                    ,day varchar(2) not null
                    ,date datetime not null );
            """
    db.getCursor().execute(query)
    db.getConn().commit()

    query = """
CREATE TABLE DimDate
	(	DateKey INT primary key,
		Date DATETIME,
		FullDateUK CHAR(10), -- Date in dd-MM-yyyy format
		FullDateUSA CHAR(10),-- Date in MM-dd-yyyy format
		DayOfMonth VARCHAR(2), -- Field will hold day number of Month
		DaySuffix VARCHAR(4), -- Apply suffix as 1st, 2nd ,3rd etc
		DayName VARCHAR(9), -- Contains name of the day, Sunday, Monday
		DayOfWeekUSA CHAR(1),-- First Day Sunday=1 and Saturday=7
		DayOfWeekUK CHAR(1),-- First Day Monday=1 and Sunday=7
		DayOfWeekInMonth VARCHAR(2), --1st Monday or 2nd Monday in Month
		DayOfWeekInYear VARCHAR(2),
		DayOfQuarter VARCHAR(3),
		DayOfYear VARCHAR(3),
		WeekOfMonth VARCHAR(1),-- Week Number of Month
		WeekOfQuarter VARCHAR(2), --Week Number of the Quarter
		WeekOfYear VARCHAR(2),--Week Number of the Year
		Month VARCHAR(2), --Number of the Month 1 to 12
		MonthName VARCHAR(9),--January, February etc
		MonthOfQuarter VARCHAR(2),-- Month Number belongs to Quarter
		Quarter CHAR(1),
		QuarterName VARCHAR(9),--First,Second..
		Year CHAR(4),-- Year value of Date stored in Row
		YearName CHAR(7), --CY 2012,CY 2013
		MonthYear CHAR(10), --Jan-2013,Feb-2013
		MMYYYY CHAR(6),
		FirstDayOfMonth DATE,
		LastDayOfMonth DATE,
		FirstDayOfQuarter DATE,
		LastDayOfQuarter DATE,
		FirstDayOfYear DATE,
		LastDayOfYear DATE,
		HolidayUSA VARCHAR(50),--Name of Holiday in US
		HolidayUK VARCHAR(50) Null --Name of Holiday in UK
	)"""


main()
