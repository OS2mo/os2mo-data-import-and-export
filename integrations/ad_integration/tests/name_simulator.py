import random

from mimesis import Person

person_gen = Person("da")


def create_name():
    names = []
    names.append(person_gen.first_name())
    if random.random() > 0.25:
        names.append(person_gen.first_name())
    if random.random() > 0.8:
        names.append(person_gen.last_name())
    if random.random() > 0.99:
        names.append(person_gen.last_name())

    names.append(person_gen.last_name())
    return names
