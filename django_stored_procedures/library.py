from django.db import models, connection

import re

class StoredProcedureLibary():
    def __init__(self):
        self._procedures = []
        self._reset = False
        self._modelLibrary = None
        self._nameRegexp = re.compile( r'\[(?P<token>[_\w]+(.[_\w]+)*)\]', re.UNICODE)

    def buildModelLibrary(self):
        nameDictionary = dict()
        quote = connection.ops.quote_name

        try:
            try:
                # django >= 1.7
                from django.apps import apps
                models = apps.get_models()
            except ImportError:
                # django < 1.7
                from django.db.models import get_model
                models = get_models()
        except LookupError:
            # both get_model versions can raise a LookupError
            models = []

        for model in models:
            meta = model._meta
            model_identifier = '%s.%s' % (meta.app_label, model.__name__)
            field_identifier = '%s.%%s' % model_identifier

            nameDictionary[model_identifier] = meta.db_table
            nameDictionary[field_identifier % 'pk'] = meta.pk.column

            for field in meta.fields:
                nameDictionary[field_identifier % field.name] = quote(field.column)

        return nameDictionary

    def replaceNames(self, sql, KeyExp):
        nameDict = self.modelLibrary

        def fill_in_names(match):
            try:
                value = nameDict[match.group('token')]
            except KeyError as exp:
                raise KeyExp(key = exp.args[0])

            return value

        return self._nameRegexp.sub(fill_in_names, sql)

    def registerProcedure(self, procedure):
        """Each stored procedure is registered with the library."""
        self._procedures.append(procedure)

    def resetProcedures(self, verbosity, force_repeat = False):
        if self._reset and not force_repeat:
            return

        self._reset = True

        for procedure in self.procedures:
            procedure.resetProcedure(
                    verbosity   = verbosity
                ,   library     = self
            )

    procedures = property(
            fget = lambda self: self._procedures
        ,   doc  = 'List of all stored procedures registered at the library'
    )

    @property
    def modelLibrary(self):
        if self._modelLibrary is None:
            self._modelLibrary = self.buildModelLibrary()

        return self._modelLibrary

library = StoredProcedureLibary()

def registerProcedure(procedure):
    """Registers a procedure with the libary."""
    library.registerProcedure(procedure)

def resetProcedures(verbosity = 2):
    """Resets all procedures registered with the library in the database."""
    library.resetProcedures(verbosity)
