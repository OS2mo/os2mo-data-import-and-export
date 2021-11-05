class ADError(Exception):
    """Superclass for AD related exceptions."""


class EngagementDatesError(ADError):
    """Error to raise when errors in engagement dates occur."""


class NoPrimaryEngagementException(ADError):
    pass


class NoActiveEngagementsException(ADError):
    pass


class UserNotFoundException(ADError):
    pass


class CprNotFoundInADException(ADError):
    pass


class ManagerNotUniqueFromCprException(ADError):
    pass


class CprNotNotUnique(ADError):
    pass


class SamAccountNameNotUnique(ADError):
    pass


class SamAccountDoesNotExist(ADError):
    pass


class ReplicationFailedException(ADError):
    pass


class NoScriptToExecuteException(ADError):
    pass


class UnknownKeywordsInScriptException(ADError):
    pass


class CommandFailure(ADError):
    pass


class ImproperlyConfigured(ADError):
    pass
