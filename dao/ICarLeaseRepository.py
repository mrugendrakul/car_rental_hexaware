from abc import ABC, abstractmethod
from model.model import Car, Customer, Lease


class CarManagement(ABC):
    @abstractmethod
    def addCar(self, car: Car):
        pass

    @abstractmethod
    def removeCar(self,carId):
        pass

    @abstractmethod
    def listAvailableCars(self):
        pass

    @abstractmethod
    def listRentedCars(self):
        pass

    @abstractmethod
    def findCarsById(self,carID):
        pass

class CustomerManagement(ABC):

    @abstractmethod
    def addCustomer(self, customer:Customer):
        pass

    @abstractmethod
    def removeCustomer(self,customerID):
        pass

    @abstractmethod
    def listCustomer(self):
        pass

    @abstractmethod
    def findCustomer(self,customerID):
        pass

class LeaseManagement(ABC):

    @abstractmethod
    def createLease(self,customerID,carID,startDate,endDate,type):
        pass

    @abstractmethod
    def returnCar(self,leaseID):
        pass

    @abstractmethod
    def listActiveLeases(self):
        pass

    @abstractmethod
    def listLeaseHistory(self):
        pass

class PaymentHandling():

    @abstractmethod
    def recordPayment(self, lease:Lease,amount,paymentDate):
        pass
