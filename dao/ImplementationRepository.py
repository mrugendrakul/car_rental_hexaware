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
        return row

    def findCarsById(self, carID):
        # conn = self.conn
        stmt = conn.cursor()
        stmt.execute(f'select * from vehicle_table where vehicleID = {carID};')
        row = stmt.fetchall()
        carRet = []
        for i in row:
            carRet.append(Car(i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7]))
        if not carRet:
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
        if not carRet:
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

        if not carRet:
            raise CarNotFoundException()
        return carRet

    def removeCar(self, carId):
        # conn = self.conn
        stmt = conn.cursor()
        stmt.execute(f'delete from vehicle_table where vehicleID = {carId};')
        row = stmt.fetchall()
        conn.commit()
        return row


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

        if not temp_cust:
            raise CustomerrNotFoundException()
        return temp_cust

    def findCustomer(self, customerID):
        stmt = conn.cursor()
        temp_cust = []
        stmt.execute(f"select * from customer_table where customerID = {customerID}")
        rows = stmt.fetchall()
        for i in rows:
            temp_cust.append(Customer(i[0], i[1], i[2], i[3], i[4]))
        return temp_cust


class LeaseManagementImplementation(LeaseManagement):
    def createLease(self, customerID, carID, startDate, endDate, type):
        stmt = conn.cursor()
        stmt.execute(
            f"insert into lease_table(vehicleId,customerID,startDate,endDate,type) VALUES ( {carID}, {customerID}, \'{startDate}\' ,\'{endDate}\' ,0)")
        row = stmt.fetchall()
        conn.commit()
        return row

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