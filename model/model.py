#Car model to use in the code
class Car:
    def __init__(self, vehicleID, make, model, Year, dailyRate, status, passenger_capacity, engine_capacity):
        self.__vehicleID = vehicleID

        self.__make = make

        self.__model = model

        self.__Year = Year

        self.__dailyRate = dailyRate

        self.__status = status

        self.__passenger_capacity = passenger_capacity

        self.__engine_capacity = engine_capacity

    #Used for getting the car details
    def get_car_details(self):
        return {"vehicleID":self.__vehicleID,"make": self.__make,"model": self.__model, "Year":self.__Year, "dailyRate":self.__dailyRate, "status" :self.__status,
                "passenger_capacity":self.__passenger_capacity,"engine_capacity" :self.__engine_capacity}

    def set_car_details(self, vehicleID, make, model, Year, dailyRate, status, passenger_capacity, engine_capacity):
        self.__vehicleID = vehicleID

        self.__make = make

        self.__model = model

        self.__Year = Year

        self.__dailyRate = dailyRate

        self.__status = status

        self.__passenger_capacity = passenger_capacity

        self.__engine_capacity = engine_capacity

    def get_vehicleID(self):
        return self.__vehicleID

    def get_make(self):
        return self.__make

    def get_model(self):
        return self.__model

    def get_Year(self):
        return self.__Year

    def get_dailyRate(self):
        return self.__dailyRate

    def get_status(self):
        return self.__status

    def get_passenger_capacity(self):
        return self.__passenger_capacity

    def get_engile_capacity(self):
        return self.__engine_capacity

#Customer model to use data
class Customer:
    def __init__(self,customerID,first_name,last_name,email,phoneNumber):
        self.__customerID = customerID
        self.__first_name = first_name
        self.__last_name = last_name
        self.__email = email
        self.__phoneNumber = phoneNumber

    def get_customer(self):
        return {"customerID":self.__customerID, "first_name":self.__first_name, "last_name":self.__last_name, "email":self.__email,"phoneNumber": self.__phoneNumber}

    def get_customerID(self):
        return self.__customerID

#Lease entity to get the data
class Lease:
    def __init__(self,leaseID ,vehicleId,customerID,startDate,endDate,type):
        self.__leaseId = leaseID
        self.__vehicleId = vehicleId
        self.__customerID = customerID
        self.__startDate = startDate
        self.__endDate = endDate
        self.__type = type

    def set_lease_id(self, leaseID):
        self.__leaseId = leaseID

    def get_lease_id(self):
        return self.__leaseId

    def get_lease(self):
        return {"leaseID":self.__leaseId,"vehicleID": self.__vehicleId,"customerID": self.__customerID,"startDate": self.__startDate,"endDate": self.__endDate,"Type": self.__type}
