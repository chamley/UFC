from dbhelper import DBHelper


def main():
    """The purpose of this script is to fully fledge out version 1.0 of our database"""
    db = DBHelper()
    create_Date_dim(db)


def create_Round_fact() -> None:
    pass


def create_Fight_dim() -> None:
    pass


def create_Fighter_dim() -> None:
    pass


def create_Date_dim(db: DBHelper) -> None:
    query = """CREATE TABLE Date_dim (
                    date_key int primary key,
                    year varchar(2),
                    month varchar(2),
                    day varchar(2),
                    timestamp datetime
            """
    db.getCursor.execute(query)
    db.getConn.commit()


main()
