class CarNotFoundException(Exception):
    def __init__(self):
        super().__init__("Car not found with the given condition")


class LeaseNotFoundException(Exception):
    def __init__(self):
        super().__init__("Lease not found with the id given")


class CustomerrNotFoundException(Exception):
    def __init__(self):
        super().__init__("Customer not found with the id given")
