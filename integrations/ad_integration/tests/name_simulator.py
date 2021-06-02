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


if __name__ == "__main__":
    import time

    t = time.time()
    for i in range(0, 1000):
        create_name()
    print(time.time() - t)
