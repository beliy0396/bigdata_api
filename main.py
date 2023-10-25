import api
import pandas as pd

API = api.DB(host='127.0.0.1', username='postgres', password='123456', database='database')

data = {'fullname': ['Петров Петр Петрович', 'Иванов Иван Иванович', 'Шиш Михаил Евгеньевич'], 'age': [111, 222, 0]}
df = pd.DataFrame(data)
API.create_table(df, 'db', 'users')
print(df)

API.delete_from_table('users', 'db', conditions="age = 0")

query = "SELECT * FROM db.users"
result = API.read_sql(query)
print(result)

API.truncate_table('db', 'users')

data = {'fullname': ['Савкин Сергей Эдуардович', 'Юнак Марк Игоревич', 'Шиш Михаил Евгеньевич'], 'age': [111, 222, 0]}
df = pd.DataFrame(data)

API.insert_sql(df, 'db', 'users')

query = "UPDATE db.users SET age = 7 WHERE fullname = 'Шиш Михаил Евгеньевич'"
API.execute(query)

query = "SELECT * FROM db.users"
result = API.read_sql(query)
print(result)

