import csv
import random

class Associates:
  def _init_(self, size):
    self.size = size

  def generateData():
    with open("associates.csv", 'w', newline = '') as file:
      writer = csv.writer(file)
      FriendRel = 0
      PersonA_ID = 0
      PersonB_ID = 0
      DateOfFriendship = 0
      Desc = ['These users are friends from college.',
              'These users are family',
              'These users are friends through their family.',
              'These users were friends when they were younger.',
              'These users are colleagues.',
              'These users are former colleagues.'
      for n in range(size):
        # PersonA_ID and PersonB_ID are links to FaceInPage table, need to be different from each other
        # Also, there can't be another row in the dataset where PersonA_ID and PersonB_ID are swapped
        FriendRel = random.randint(1,20000000)
        DateOfFriendship = random.randint(1,1000000)
        writer.writerow(FriendRel,PersonA_ID,PersonB_ID,DateOfFriendship,random.choice(Desc))
        
