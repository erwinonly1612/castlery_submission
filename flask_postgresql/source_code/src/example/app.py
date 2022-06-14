import json

from flask import request

from . import create_app, database
from .models import Employees
import pandas as pd
app = create_app()


@app.route('/employee', methods=['GET'])
def fetch():
    employees = database.get_all(Employees)
    all_employees = []
    for employee in employees:
        new_employee = {
            "id": employee.id,
            "employee": employee.employee,
            "gender": employee.gender,
            "age": employee.age,
            "salary": employee.salary,
            "town": employee.town,
        }

        all_employees.append(new_employee)
    return json.dumps(all_employees), 200

@app.route('/employee/<employee_id>', methods=['GET'])
def fetch_single(employee_id):
    employee = database.get_instance(Employees,employee_id)
    new_employee = {
        "id": employee.id,
        "employee": employee.employee,
        "gender": employee.gender,
        "age": employee.age,
        "salary": employee.salary,
        "town": employee.town,
    }
    return json.dumps(new_employee), 200


@app.route('/employee', methods=['POST'])
def add():
    data = request.get_json()
    employee = data['employee']
    gender = data['gender']
    age = data['age']
    salary = data['salary']
    town = data['town']

    database.add_instance(Employees, employee=employee, gender=gender, age=age, salary=salary, town=town)
    return json.dumps("Added"), 200


@app.route('/employee/<employee_id>', methods=['DELETE'])
def remove(employee_id):
    database.delete_instance(Employees, id=employee_id)
    return json.dumps("Deleted"), 200


@app.route('/employee/<employee_id>', methods=['POST'])
def edit(employee_id):
    data = request.get_json()
    new_employee = data['employee']
    new_gender = data['gender']
    new_age = data['age']
    new_salary = data['salary']
    new_town = data['town']
    database.edit_instance(Employees, id=employee_id, employee=new_employee, gender=new_gender, age=new_age, salary=new_salary, town=new_town)
    return json.dumps("Edited"), 200
    
@app.route('/employee/import_csv', methods=['GET'])
def import_csv():
    employees_df = pd.read_csv('./src/example/employees.csv')
    employees_df['id'] = employees_df['id'].astype('int64')
    employees_df['age'] = employees_df['age'].astype('int64')
    employees_df['salary'] = employees_df['salary'].astype('float')

    for index,data in employees_df.iterrows():
        employee = data['employee']
        gender = data['gender']
        age = data['age']
        salary = data['salary']
        town = data['town']
        database.add_instance(Employees, employee=employee, gender=gender, age=age, salary=salary, town=town)

    return json.dumps("csv imported"), 200
