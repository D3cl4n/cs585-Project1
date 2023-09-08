import os
import time
import random

class LogsGeneration:
    def __init__(self, size, csv_path):
        self.size = size
        self.dataset_path = csv_path
        self.possible_people_ids = []
        self.possible_page_ids = []
        self.output_path = os.getcwd() + "access_logs.csv"
        self.access_types = [
            "note", "added a friend", "just viewed", "requested to follow",
            "accepted follow request", "shared a post", "blocked the profile",
            "viewed mutual followers", "sent a message"
        ]

    def read_first_csv(self): # will return tuple of person ID 
        with open(self.dataset_path, "r") as f:
            pass
    
    def create_output_csv(self):
        with open(self.output_path, "w") as f:
            f.write("AccessId, ByWho, WhatPage, TypeOfAccess, AccessTime\n")
        f.close()

    def write_to_csv(self, entry):
        with open(self.output_path, "a+") as f:
            f.write(entry + "\n")
        f.close()

    def format_entry(self, log_entry):
        final_entry = ""
        for x in range(0, len(log_entry)):
            final_entry += str(log_entry[x]) + ","
        
        return final_entry[:-1]


    def create_entry(self):
        access_id_lower = 1
        access_id_upper = 10000000 
        by_who = 2
        what_page = 3
        access_id = random.randint(access_id_lower, access_id_upper)
        idx = random.randint(0, len(self.access_types) - 1)
        access_type = self.access_types[idx]
        curr_time = time.time()
        access_time = time.ctime(curr_time)
        entry = [access_id, by_who, what_page, access_type, access_time]
        final_entry = self.format_entry(entry)

        self.write_to_csv(final_entry)


    def generate_data(self):
        self.create_output_csv()
        for x in range(0, self.size):
            self.create_entry()

if __name__ == '__main__':
    test_code = LogsGeneration(10, "test")
    test_code.generate_data()