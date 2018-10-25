# -*- coding: utf-8 -*-
class GrpcError(Exception):
    def __init__(self, status, details, debug_error_string):
        self.status = status
        self.details = details
        self.debug_error_string = debug_error_string

    def __str__(self):
        return (
            "<RPC terminated with:\n"
            "\tstatus = {}\n"
            '\tdetails = "{}"\n'
            '\tdebug_error_string = "{}"\n'
            ">".format(self.status, self.details, self.debug_error_string)
        )
