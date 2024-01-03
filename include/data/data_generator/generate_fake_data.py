from fake_data_generator import FakeDataGenerator
import csv

def load_existing_data(file_path):
    existing_data = []
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            existing_data.append(row)
    return existing_data

def generate_fake_data():
    existing_data_path = 'your_file.csv'
    existing_data = load_existing_data(existing_data_path)
    fake_data_generator = FakeDataGenerator(existing_data)
    fake_data = fake_data_generator.generate_fake_data()
    return fake_data
    
