from csv import Error
from BaseX.BaseXClient import BaseXClient

try:
    session = BaseXClient.Session("localhost", 8080, "admin", "admin")
    print("connected!! yeppy")
except Error as e:
    print(e)
