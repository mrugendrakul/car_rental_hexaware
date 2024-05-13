import unittest

from Exception.Exceptions import CarNotFoundException
from dao.ImplementationRepository import CarManagementImplementation,LeaseManagementImplementation
from model.model import Car


class MyTestCases(unittest.TestCase):
    def setUp(self):
        self.car = CarManagementImplementation()
        self.lease_manage = LeaseManagementImplementation()
    def test_car_create(self):
        result = self.car.addCar(Car(0, make="alto", model="maruti", Year="2024", dailyRate="25", status="1", passenger_capacity="25", engine_capacity="2456"))

        self.assertEqual(result, "Car added successfully")

    def test_lease_creation(self):
        result = self.lease_manage.createLease(customerID=200,  startDate = "2024-05-01", endDate="2024-06-01", type="1",carID=3)
        self.assertEqual(result,"Lease created successfully")

    def test_lease_retrive(self):
        result = self.lease_manage.listLeaseHistory()
        self.assertNotEqual(len(result),0)

    def test_find_car(self):
        with self.assertRaises(CarNotFoundException):
            self.car.findCarsById(110)
