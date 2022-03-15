def mod5(session, x):
    return session.sql(f"SELECT {x} % 5").collect()[0][0]
