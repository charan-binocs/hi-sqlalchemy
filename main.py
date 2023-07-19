from typing import Optional
import sqlalchemy as sa
import sqlalchemy.orm as sa_orm
import datetime as dt
import sqlmodel as sm

engine = sa.create_engine(url="sqlite:///sqlite.db", echo=True)


# Using primitive SQL text

conn = engine.connect()
conn.execute(
    sa.text(
        """-- Create table `user`
CREATE TABLE user (
    id INTEGER PRIMARY KEY,
    name TEXT,
    dob DATETIME
)
"""
    )
)

conn.execute(
    sa.text(
        """-- Add user
INSERT INTO user (name, dob)
VALUES (:name, :dob)
"""
    ).bindparams(
        name="Alice Almeron",
        dob=dt.datetime.fromisoformat("2000-01-01T13:59:00.000+05:30"),
    )
)


result = conn.execute(
    sa.text(
        """-- Get all users
SELECT u.id, u.name, u.dob
FROM user u
"""
    )
)

for user in result:
    print(user)
    print(user[0], user[1], user[2])
    print(user.id, user.name, user.dob)
    print()


conn.execute(sa.text("DROP TABLE user"))


# Using SA Core

metadata = sa.MetaData()

user_table = sa.Table(
    "user",
    metadata,
    sa.Column("id", sa.Integer, primary_key=True),
    sa.Column("name", sa.String),
    sa.Column("dob", sa.DateTime),
)

user_table.create(bind=conn)

conn.execute(
    user_table.insert().values(
        [
            dict(
                name="Alice Almeron",
                dob=dt.datetime.fromisoformat("2000-12-13T14:00:30+05:30"),
            ),
            dict(
                name="Bob Baker",
                dob=dt.datetime.fromisoformat("2000-04-23T16:34:53+05:30"),
            ),
        ]
    )
)


users = conn.execute(user_table.select().where(user_table.c.id < 1_000))

for user in users:
    print(user)
    print(user[0], user[1], user[2])
    print(user.id, user.name, user.dob)


user_table.drop(bind=conn)

conn.close()


# Using ORM

session = sa_orm.sessionmaker(bind=engine)

Base = sa_orm.declarative_base()


class User(Base):
    __tablename__: str = "user"

    id: sa_orm.Mapped[int] = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
    name: sa_orm.Mapped[str] = sa.Column(sa.String)
    dob: sa_orm.Mapped[dt.datetime] = sa.Column(sa.DateTime)

    def __repr__(self):
        _ = self
        return f"User(id={_.id}, name={_.name})"


with session() as s:
    Base.metadata.create_all(bind=s.connection())

    user_1 = User(
        name="Alice Almeron",
        dob=dt.datetime.fromisoformat("1999-11-30T14:00:39.293200+05:30"),
    )
    s.add(user_1)
    s.flush()

    user_2 = User(
        name="Bob Baker", dob=dt.datetime.fromisoformat("2001-04-01T03:40:02+05:30")
    )
    s.add(user_2)
    User()
    users = s.execute(sa.select(User)).scalars().all()
    for user in users:
        print(user)
        print(user.id, user.name, user.dob)

    Base.metadata.drop_all(bind=s.connection())

    s.commit()


# Using sqlmodel


class User(sm.SQLModel, table=True):
    id: Optional[int] = sm.Field(primary_key=True)
    name: str
    dob: dt.datetime


with sm.Session(bind=engine) as s:
    sm.SQLModel.metadata.create_all(bind=s.connection())

    user_1 = User(
        name="Alice", dob=dt.datetime.fromisoformat("2023-01-05T23:44:18+05:30")
    )
    user_2 = User(
        name="Bob", dob=dt.datetime.fromisoformat("2023-01-05T23:44:18+05:30")
    )

    s.add(user_1)
    s.flush()

    users = s.exec(sm.select(User).where(sm.col(User.id) < 100))
    for user in users:
        print(user)
        print(user.id, user.name, user.dob)

    s.add(user_2)

    s.commit()

    user_2 = s.get(User, 2)

    print(user_2)

    sm.SQLModel.metadata.drop_all(bind=s.connection())
    