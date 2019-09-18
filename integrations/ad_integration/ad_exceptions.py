class NoPrimaryEngagementException(Exception):
    pass


class UserNotFoundException(Exception):
    pass


class ManagerNotUniqueFromCprException(Exception):
    pass


class CprNotNotUnique(Exception):
    pass


class SamAccountNameNotUnique(Exception):
    pass


class SamAccountDoesNotExist(Exception):
    pass


class ReplicationFailedException(Exception):
    pass
