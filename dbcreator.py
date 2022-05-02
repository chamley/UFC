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


def create_Date_fim() -> None:
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


main()
