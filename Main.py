from dao.ImplementationRepository import *

while True:
    # option = 0
    print("Please select the options")
    print("1) Customer Operations")
    print("2) Vehicle Operations")
    print("3) Lease Operations")
    print("4) Payment Operations")
    print("5) End")

    car_manage = CarManagementImplementation()
    customer_manage = CustomerManagementImplementation()
    lease_manage = LeaseManagementImplementation()
    payment_manage = PaymentManagementImplementation()

    option = int(input("Option : "))

    if option == 5:
        break

    #customer options
    elif option == 1:
        print()
        print("1) Add Customer")
        print("2) Remove Customer")
        print("3) List Customer")
        print("4) Find Customer")
        print("5) Back")
        sub_option = int(input("Option : "))

        # Add customer
        if sub_option == 1 :
            print("Please Give infor for the customer")

            first_name = input("First Name : ")
            last_name = input("Last Name : ")
            email = input("Email : ")
            phoneNumber = input("Phone Number : ")
            try:
                result = customer_manage.addCustomer(customer=Customer(0, first_name, last_name, email, phoneNumber))
                print(result)
                print("User added successfully !")
            except Exception as e:
                print(f"Error encountered while adding the customers : {e}")

        # Remove Customer
        elif sub_option == 2:
            customerID = int(input("User ID to remove (Please verify from the list customer option) : "))

            try:
                result = customer_manage.removeCustomer(customerID)
                print(result)
                print("User removed successfully !")
            except Exception as e:
                print(f"Error encountered while adding the customers : {e}")

        # List Customers
        elif sub_option == 3:
            try:
                result = customer_manage.listCustomer()
                for customer in result:
                    print(customer.get_customer())
            except Exception as e:
                print(f"Error encountered while listing the customers : {e}")


        # Find Customers
        elif sub_option == 4:
            customerID = int(input("Customer to find : "))
            try:
                result = customer_manage.findCustomer(customerID)
                for customer in result:
                    print(customer.get_customer())
            except Exception as e:
                print(f"Error encountered while finding customers : {e}")

        # continue
        elif sub_option == 5:
            continue
        else:
            print("select valid option")

    # Car Options
    elif option == 2:
        print()
        print("Select options")
        print("1) Add Car")
        print("2) Find car by id")
        print("3) List Rented car")
        print("4) List Available car")
        print("5) Remove car")
        print("6) Back")

        sub_option = int(input("Option : "))

        #Add Car
        if sub_option == 1:
            print("Give the following information")
            make = input("Make of the car : ")
            model = input("Model of the car : ")
            Year = input("Year of the car : ")
            dailyRate = input("Daily Rate : ")
            status = int(input("Available(1) or unavailable(0)"))
            passenger_capacity = input("Passenger capacity of the car : ")
            engine_capacity = input("Engine Capacity of the car : ")

            try:
                result = car_manage.addCar(Car(0, make, model, Year, dailyRate, status, passenger_capacity, engine_capacity))
                print(result)
                print("Successfully added the car!")
            except Exception as e:
                print(f"Error encountered while adding the car : {e}")

        # find car by id
        if sub_option == 2 :
            vehicleID = int(input("Give car ID : "))

            try:
                cars = car_manage.findCarsById(vehicleID)
                for car in cars:
                    print(car.get_car_details())

            except Exception as e:
                print(f"Error encountered while getting cars : {e}")

        #List Rented car
        elif sub_option == 3:
            print("List of the rented cars are : ")
            try:
                cars = car_manage.listRentedCars()
                for car in cars:
                    print(car.get_car_details())

            except Exception as e:
                print(f"Error encountered while getting cars : {e}")

        #List avialabel car
        elif sub_option == 4:
            print("List of the available cars are : ")
            try:
                cars = car_manage.listAvailableCars()
                for car in cars:
                    print(car.get_car_details())

            except Exception as e:
                print(f"Error encountered while getting cars : {e}")

        #Remove car
        elif sub_option == 5:
            carID = int(input("Car ID to remove (Verify from list cars) : "))
            try:
                result = car_manage.removeCar(carID)
                print("Successfully removed car")
            except Exception as e:
                print(f"Error encountered while removing car : {e}")

        elif sub_option == 6:
            continue

        else:
            print("Select valid option")
    # Lease Options
    elif option == 3:
        print()
        print("Select Options")
        print("1) Create new Lease")
        print("2) Get Lease ")
        print("3) List Active Lease")
        print("4) List past Lease")
        print("5) Back")

        sub_option = int(input("Option : "))
        #Create new lease
        if sub_option == 1:
            print("Fill following options")
            vehicleId = input("Vehicle ID (select from the vehicle list) : ")
            customerID = input("Customer ID (select from customer list) : ")
            startDate = input("Start Date (yyyy-mm-dd) : ")
            endDate = input("End date (yyyy-mm-dd) : ")
            type = input("dailyBased(0) monthlyBased (1) : ")

            try:
                ret = lease_manage.createLease(customerID, vehicleId, startDate, endDate, type)
                print(ret)
                print("Successfully created lease !")
            except Exception as e:
                print(f"Error encountered while adding lease : {e}")

        #Get Lease
        elif sub_option == 2:
            leaseID = int(input("Lease ID : "))

            try:
                leases = lease_manage.returnCar(leaseID)
                for lease in leases:
                    print(lease.get_lease())
            except Exception as e:
                print(f"Error encountered while getting lease : {e}")

        # List Active leases
        elif sub_option ==3:
            print("Active leases are")

            try:
                leases = lease_manage.listActiveLeases()
                for lease in leases:
                    print(lease.get_lease())
            except Exception as e:
                print(f"Error encountered while getting lease : {e}")

        #List Past leases
        elif sub_option == 4:
            print("Past leases are")

            try:
                leases = lease_manage.listLeaseHistory()
                for lease in leases:
                    print(lease.get_lease())
            except Exception as e:
                print(f"Error encountered while getting lease : {e}")

        #continue
        elif sub_option ==5:
            continue

        else:
            print("select valid option")

    #Payment operations
    elif option == 4:
        print("Options")
        print("1) New payment")
        print("2) List payment")
        print("3) Back")

        sub_option = int(input("Option : "))

        #New payment
        if sub_option == 1:
            print("Give following information")
            leaseID = int(input("LeaseID (verify from lease list) : "))
            paymentDate = input("Payment Date (yyyy-mm-dd)")
            amount = int(input("Amount : "))

            try:
                result = payment_manage.recordPayment(leaseID,amount,paymentDate)
                print(result)
                print("Successfully added payment!")
            except Exception as e:
                print(f"Error encountered while adding payment : {e}")

        #List Payment
        elif sub_option ==2:
            print("All payments are")
            try:
                result = payment_manage.getPayment()
                for pay in result:
                    print(pay)

            except Exception as e:
                print(f"Error encountered while adding payment : {e}")

        elif sub_option == 5:
            continue

        else:
            print("select valid option")

    else:
        print("select valid option")