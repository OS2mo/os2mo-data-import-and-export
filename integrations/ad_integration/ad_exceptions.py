class NoPrimaryEngagementException(Exception):
    pass

class NoActiveEngagementsException(Exception):
    pass


class UserNotFoundException(Exception):
    pass

class CprNotFoundInADException(Exception):
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


class NoScriptToExecuteException(Exception):
    pass


class UnknownKeywordsInScriptException(Exception):
    pass
