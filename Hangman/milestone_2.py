import random
word_list = ['Apple', 'Orange', 'Grape', 'Strawberry', 'Raspberry']
word = random.choice(word_list)
print(word)

guess = input("Input a single letter:")

for letter in guess:
    if letter.isalpha() and len(letter) == 1:
        print("Good guess!")
    else:
       print("Oops! That is not a valid input.")
