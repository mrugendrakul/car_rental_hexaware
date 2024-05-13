from Exception.Exceptions import CarNotFoundException, CustomerrNotFoundException
from dao.ICarLeaseRepository import *
from util.PropertyUtil import DBConnUtil

conn = DBConnUtil.makeConnection()


class CarManagementImplementation(CarManagement):
    # conn = DBConnUtil.makeConnection()

    def addCar(self, car: Car):
        # conn = self.conn
        carInfo = car.get_car_details()
        stmt = conn.cursor()
        stmt.execute(
            f'insert into vehicle_table(make,model,Year,dailyRate,status,passenger_capacity, engine_capacity) VALUES (\'{carInfo['make']}\',\'{carInfo['model']}\',\'{carInfo['Year']}-01-01\',{carInfo['dailyRate']},{carInfo['status']},{carInfo['passenger_capacity']},{carInfo['engine_capacity']});')
        row = stmt.fetchall()
        conn.commit()
        return "Car added successfully"

    def findCarsById(self, carID):
        # conn = self.conn
        stmt = conn.cursor()
        stmt.execute(f'select * from vehicle_table where vehicleID = {carID};')
        row = stmt.fetchall()
        carRet = []
        for i in row:
            carRet.append(Car(i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7]))
        if len(carRet) == 0:
            raise CarNotFoundException()
        return carRet

    def listAvailableCars(self):
        # conn = self.conn
        stmt = conn.cursor()
        stmt.execute(f'select * from vehicle_table where status = 1;')
        row = stmt.fetchall()
        carRet = []
        for i in row:
            carRet.append(Car(i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7]))
        if len(carRet) == 0:
            raise CarNotFoundException()
        return carRet

    def listRentedCars(self):
        # conn = self.conn
        stmt = conn.cursor()
        stmt.execute(f'select * from vehicle_table where status = 0;')
        row = stmt.fetchall()
        carRet = []
        for i in row:
            carRet.append(Car(i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7]))
        if len(carRet) == 0:
            raise CarNotFoundException()
        return carRet

    def removeCar(self, carId):
        self.findCarsById(carId)
        # conn = self.conn
        stmt = conn.cursor()
        stmt.execute(f'delete from vehicle_table where vehicleID = {carId};')
        row = stmt.fetchall()
        conn.commit()
        return row

    def update_car(self,carID):
        cars = self.findCarsById(carID)
        for car in cars:
            print(car.get_car_details())
        print("Select value to update ")
        print("1) Make")
        print("2) Model")
        print("3) Year")
        print("4) Daily Rate")
        print("5) Status")
        print("6) Passenger Capacity")
        print("7) Engine Capacity")
        val_change = "not selected "
        option = int(input("Option : "))
        if option == 1:
            val_change = "make"
        elif option == 2:
            val_change = "model"
        elif option == 3:
            val_change = "Year"
        elif option == 4:
            val_change = "dailyRate"
        elif option == 5:
            val_change = "status"
        elif option == 6:
            val_change = "passenger_capacity"
        elif option == 7:
            val_change = "engine_capacity"

        value = input(f"New Value for the {val_change} : ")
        if option == 3:
            value = value + "-01-01"
        stmt = conn.cursor()

        stmt.execute(f"update vehicle_table set {val_change} = '{value}' where vehicleID = {carID};")
        conn.commit()
        print("Value updated successfully")


class CustomerManagementImplementation(CustomerManagement):

    def addCustomer(self, customer: Customer):
        # conn = self.conn
        stmt = conn.cursor()
        cust_info = customer.get_customer()
        stmt.execute(
            f"insert into customer_table(first_name,last_name,email,phoneNumber) VALUES (\'{cust_info["first_name"]}\', \'{cust_info["last_name"]}\', \'{cust_info["email"]}\' ,\'{cust_info["phoneNumber"]}\');")
        row = stmt.fetchall()
        conn.commit()
        return row

    def removeCustomer(self, customerID):
        self.findCustomer(customerID)
        # conn = self.conn
        stmt = conn.cursor()
        stmt.execute(f"delete from customer_table where customerID = {customerID};")
        row = stmt.fetchall()
        conn.commit()
        return row

    def listCustomer(self):
        # conn = self.conn
        stmt = conn.cursor()
        temp_cust = []
        stmt.execute(f"select * from customer_table")
        rows = stmt.fetchall()
        for i in rows:
            temp_cust.append(Customer(i[0], i[1], i[2], i[3], i[4]))

        if len(temp_cust) == 0:
            raise CustomerrNotFoundException()
        return temp_cust

    def update_customer(self,customer_id):
        self.findCustomer(customer_id)
        print("Select value to update ")
        print("1) First name")
        print("2) Last name")
        print("3) Email")
        print("4) Phone Number")
        val_change = "not selected "
        option = int(input("Option : "))
        if option == 1:
            val_change = "first_name"
        elif option == 2:
            val_change = "last_name"
        elif option == 3:
            val_change = "email"
        elif option == 4:
            val_change = "phone_number"

        value = input(f"New Value for the {val_change} : ")
        stmt = conn.cursor()

        stmt.execute(f"update customer_table set {val_change} = '{value}' where customerID = {customer_id};")
        conn.commit()
        print("Value updated successfully")

    def delete_customer(self,customerID):
        self.findCustomer(customerID)
        stmt = conn.cursor()
        stmt.execute(f"delete from customer_table where customerID = {customerID};")
        conn.commit()
        print("Customer Deleted Successfully")

    def findCustomer(self, customerID):
        stmt = conn.cursor()
        temp_cust = []
        stmt.execute(f"select * from customer_table where customerID = {customerID}")
        rows = stmt.fetchall()
        if len(rows) == 0:
            raise CustomerrNotFoundException()
        for i in rows:
            temp_cust.append(Customer(i[0], i[1], i[2], i[3], i[4]))
        return temp_cust


class LeaseManagementImplementation(LeaseManagement):
    def createLease(self, customerID, carID, startDate, endDate, type):
        stmt = conn.cursor()
        try:
            stmt.execute(
            f"insert into lease_table(vehicleId,customerID,startDate,endDate,type) VALUES ( {carID}, {customerID}, \'{startDate}\' ,\'{endDate}\' ,0)")
        except Exception as e:
            print(f"Error while creating lease : {e}")
        row = stmt.fetchall()
        conn.commit()
        return "Lease created successfully"

    def returnCar(self, leaseID):
        stmt = conn.cursor()
        temp_lease = []
        stmt.execute(f"select * from lease_table where leaseID = {leaseID};")
        rows = stmt.fetchall()
        for i in rows:
            temp_lease.append(Lease(i[0], i[1], i[2], i[3], i[4], i[5]))
        return temp_lease

    def listActiveLeases(self):
        stmt = conn.cursor()
        temp_lease = []
        stmt.execute(f"select * from lease_table where endDate > current_date();")
        rows = stmt.fetchall()
        for i in rows:
            temp_lease.append(Lease(i[0], i[1], i[2], i[3], i[4], i[5]))
        return temp_lease

    def listLeaseHistory(self):
        stmt = conn.cursor()
        temp_lease = []
        stmt.execute(f"select * from lease_table where endDate < current_date();")
        rows = stmt.fetchall()
        for i in rows:
            temp_lease.append(Lease(i[0], i[1], i[2], i[3], i[4], i[5]))
        return temp_lease


class PaymentManagementImplementation(PaymentHandling):

    def recordPayment(self, leaseID, amount, paymentDate):
        stmt = conn.cursor()
        stmt.execute(
            f"insert into payment_table (leaseID,paymentDate,amount) VALUES ({leaseID} ,\'{paymentDate}\', {amount});")
        row = stmt.fetchall()
        conn.commit()
        return row

    def getPayment(self):
        stmt = conn.cursor()
        temp_pay = []
        stmt.execute(f"select * from payment_table;")
        rows = stmt.fetchall()
        for i in rows:
            temp_pay.append({"paymentID": i[0] ,"leaseID":i[1], "paymentDate":i[2], "amount": i[3]})
        return temp_pay