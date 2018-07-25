class GmSvrNotifyError(Exception):
    """Notify Gmsvr fail"""


class GmSvrHttpError(GmSvrNotifyError):
    """Http request error """


class GmSvrNotifyCancel(GmSvrNotifyError):
    """Notify thread cancel, overtime?"""


class GmSvrNotifyNotExec(GmSvrNotifyError):
    """Notify not Execute, async reqpone fail?"""


class MergeException(Exception):
    """Merge fail exception"""
