import uuid
from faker import Faker
from random import randint, choice
from datetime import datetime, timedelta

fake = Faker()

class MockDataGenerator:
    @staticmethod
    def generate_countries(n):
        return [
            {
                "country_id": str(uuid.uuid4()),
                "country": fake.country()
            } for _ in range(n)
        ]
    @staticmethod
    def generate_states(n, countries):
        return [
            {
                "state_id": str(uuid.uuid4()),
                "state": fake.state(),
                "country_id": choice(countries)["country_id"]
            } for _ in range(n)
        ]

    @staticmethod
    def generate_cities(n, states):
        return [
            {
                "city_id": i + 1,
                "city": fake.city(),
                "state_id": choice(states)["state_id"]
            } for i in range(n)
        ]

    @staticmethod
    def generate_customers(n, cities):
        return [
            {
                "customer_id": str(uuid.uuid4()),
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "customer_city": choice(cities)["city_id"],
                "country_name": fake.country(),
                "cpf": randint(10000000000, 99999999999)
            } for _ in range(n)
        ]

    @staticmethod
    def generate_accounts(n, customers):
        statuses = ["active", "inactive", "closed"]
        return [
            {
                "account_id": str(uuid.uuid4()),
                "customer_id": choice(customers)["customer_id"],
                "created_at": fake.date_time_between(start_date="-2y", end_date="now"),
                "status": choice(statuses),
                "account_branch": fake.bban(),
                "account_check_digit": str(randint(0, 99)),
                "account_number": str(randint(100000, 999999))
            } for _ in range(n)
        ]

    @staticmethod
    def generate_transfer_ins(n, accounts):
        statuses = ["pending", "completed", "failed"]
        return [
            {
                "id": str(uuid.uuid4()),
                "account_id": choice(accounts)["account_id"],
                "amount": round(fake.random_number(digits=5, fix_len=True) / 100, 2),
                "transaction_requested_at": randint(1, 1000),
                "transaction_completed_at": randint(1001, 2000),
                "status": choice(statuses)
            } for _ in range(n)
        ]

    @staticmethod
    def generate_transfer_outs(n, accounts):
        statuses = ["pending", "completed", "failed"]
        return [
            {
                "id": str(uuid.uuid4()),
                "account_id": choice(accounts)["account_id"],
                "amount": round(fake.random_number(digits=5, fix_len=True) / 100, 2),
                "transaction_requested_at": randint(1, 1000),
                "transaction_completed_at": randint(1001, 2000),
                "status": choice(statuses)
            } for _ in range(n)
        ]

    @staticmethod
    def generate_pix_movements(n, accounts):
        statuses = ["pending", "completed", "failed"]
        directions = ["in", "out"]
        return [
            {
                "id": str(uuid.uuid4()),
                "account_id": choice(accounts)["account_id"],
                "in_or_out": choice(directions),
                "pix_amount": round(fake.random_number(digits=5, fix_len=True) / 100, 2),
                "pix_requested_at": randint(1, 1000),
                "pix_completed_at": randint(1001, 2000),
                "status": choice(statuses)
            } for _ in range(n)
        ]

    @staticmethod
    def generate_d_month(n):
        return [
            {
                "month_id": i + 1,
                "action_month": randint(1, 12)
            } for i in range(n)
        ]

    @staticmethod
    def generate_d_year(n):
        current_year = datetime.now().year
        return [
            {
                "year_id": i + 1,
                "action_year": current_year - i
            } for i in range(n)
        ]

    @staticmethod
    def generate_d_week(n):
        return [
            {
                "week_id": i + 1,
                "action_week": randint(1, 52)
            } for i in range(n)
        ]

    @staticmethod
    def generate_d_weekday(n):
        weekdays = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        return [
            {
                "weekday_id": i + 1,
                "action_weekday": choice(weekdays)
            } for i in range(n)
        ]

    @staticmethod
    def generate_d_time(n, weeks, months, years, weekdays):
        base_time = datetime.now() - timedelta(days=365)
        return [
            {
                "time_id": i + 1,
                "action_timestamp": base_time + timedelta(seconds=i * 3600),
                "week_id": choice(weeks)["week_id"],
                "month_id": choice(months)["month_id"],
                "year_id": choice(years)["year_id"],
                "weekday_id": choice(weekdays)["weekday_id"]
            } for i in range(n)
        ]
    

#Example to generate all mocks and store in memory

# if __name__ == "main":
#     countries = MockDataGenerator.generate_countries(5)
#     states = MockDataGenerator.generate_states(10, countries)
#     cities = MockDataGenerator.generate_cities(20, states)
#     customers = MockDataGenerator.generate_customers(15, cities)
#     accounts = MockDataGenerator.generate_accounts(10, customers)
#     transfer_ins = MockDataGenerator.generate_transfer_ins(20, accounts)
#     transfer_outs = MockDataGenerator.generate_transfer_outs(20, accounts)
#     pix_movements = MockDataGenerator.generate_pix_movements(20, accounts)
#     d_month = MockDataGenerator.generate_d_month(12)
#     d_year = MockDataGenerator.generate_d_year(5)
#     d_week = MockDataGenerator.generate_d_week(52)
#     d_weekday = MockDataGenerator.generate_d_weekday(7)
#     d_time = MockDataGenerator.generate_d_time(100, d_week, d_month, d_year, d_weekday)
# Print a sample of each mock data for verification
# print("Countries:", countries[:2])
# print("States:", states[:2])
# print("Cities:", cities[:2])
# print("Customers:", customers[:2])
# print("Accounts:", accounts[:2])
# print("Transfer Ins:", transfer_ins[:2])
# print("Transfer Outs:", transfer_outs[:2])
# print("Pix Movements:", pix_movements[:2])
# print("Months:", d_month[:2])
# print("Years:", d_year[:2])
# print("Weeks:", d_week[:2])
# print("Weekdays:", d_weekday[:2])
# print("Time:", d_time[:2])