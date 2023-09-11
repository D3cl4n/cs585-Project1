import csv
import random

class Associates:
  def __init__(self, size):
    self.size = size

  def generateData(self):

    # Read FaceIn.csv and store all the IDs for later when we generate Associates.csv
    FaceInIDs = []
    with open('FaceIn.csv', newline = '') as faceInFile:
        reader = csv.DictReader(faceInFile)
        for row in reader:
            FaceInIDs.append(row['ID'])

    with open("associates.csv", 'w', newline = '') as file:
      writer = csv.writer(file)
      FriendRel = 0
      PersonA_ID = 0
      PersonB_ID = 0
      IDPairs = []
      DateOfFriendship = 0
      Desc = ['These users are friends from college.',
              'These users are family',
              'These users are friends through their family.',
              'These users were friends when they were younger.',
              'These users are colleagues.',
              'These users are former colleagues.']
      writer.writerow(["FriendRel","PersonA_ID","PersonB_ID","DateOfFriendship","Desc"])
      for n in range(self.size):
        # PersonA_ID and PersonB_ID are links to FaceInPage table, need to be different from each other
        # Also, there can't be another row in the dataset where PersonA_ID and PersonB_ID are swapped
        FriendRel = random.randint(1,20000000)
        DateOfFriendship = random.randint(1,1000000)

        # Randomly choose the IDs for PersonA and PersonB
        # If the IDs are the same or if there is already a record where the two IDs are linked,
        # then the second ID should be reselected.
        PersonA_ID = random.choice(FaceInIDs)
        PersonB_ID = random.choice(FaceInIDs)
        while PersonA_ID == PersonB_ID or (PersonA_ID,PersonB_ID) in IDPairs or (PersonB_ID,PersonA_ID) in IDPairs:
            PersonB_ID = random.choice(FaceInIDs)

        IDPairs.append((PersonA_ID,PersonB_ID))

        writerArg = [FriendRel,PersonA_ID,PersonB_ID,DateOfFriendship,random.choice(Desc)]
        writer.writerow(writerArg)


