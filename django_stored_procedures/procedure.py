try:
    from django.db import connection
    from django.db.utils import DatabaseError
    from django.conf import settings
except ImportError as exp:
    print exp

from django.template import Template, Context
from django.db.utils import OperationalError

import codecs, itertools, re, functools, warnings

from exceptions import *
from library import registerProcedure

IN_OUT_STRING  = '(IN)|(OUT)|(INOUT)'
argumentString = r'(?P<inout>' + IN_OUT_STRING + ')\s*(?P<name>[\w_]+)\s+(?P<type>.+?(?=(,\s*' + IN_OUT_STRING + ')|$))'
argumentParser = re.compile(argumentString, re.DOTALL)

methodParser = re.compile(r'CREATE\s+PROCEDURE\s+(?P<name>[\w_]+)\s*\(\s*(?P<arguments>.*)\)[^\)]*BEGIN', re.DOTALL)

class StoredProcedure():
    def __init__(
                self
            ,   filename
            ,   name            = None
            ,   arguments       = None
            ,   results         = False
            ,   flatten         = True
            ,   context         = None
            ,   raise_warnings  = False
    ):
        """Make a wrapper for a stored procedure

:param filename: The file where the stored procedure's content is stored.
:type filename: str or unicode
:param arguments: A list of the argument the procedure needs.
:type arguments: list of strings.
:param results: whether the procedure yields a resultset (default is `False`)
:type results: `bool`
:param flatten: whether the resultset, whenever available, should be flattened to its first element, ueful when the procedure only returns one row (default `True`)
:type flatten: bool
:param context: a context (dictionary or function which takes the stored procedure itself and yields a dictionary) for rendering the procedure (default is empty)
:param raise_warnings: whether warnings should be raised as an exception (default is false)
:type raise_warnings: bool
:raises: :exc:`~exceptions.InitializationException` in case one of the arguments does not satisfy the above description or :exc:`~exceptions.FileDoesNotWorkException` in case :meth:`~procedure.StoredProcedure.readProcedure` fails. If you can not differentiate between these errors in handling them (as would be most common), simply check for :exc:`~exceptions.ProcedureConfigurationException`, as this is a parent of both.

This provides a wrapper for stored procedures. Given the location of a stored procedure, this wrapper can automatically infer its arguments and name. Consequently, one can call the wrapper as if it were a function, using these arguments as keyword arguments, resulting in calling the stored procedure.

By default, the stored procedure will be stored in the database (replacing any stored procedure with the same name) on a django-south migrate event.

It is possible to refer to models and columns of models from within the stored procedure in the following sense. If in the application "shop" one has a model named "Stock", then writing [shop.Stock] in the file describing the stored procedure will yield a the database-name of the model Stock. If this model has a field "shelf", then [shop.Stock.shelf] will yield the field's database name. As a shortcut, one can also use [shop.Stock.pk] to refer to the primary key of Stock. All these names are escaped appropriately.

Moreover, one can use django templating language in the stored procedure. The argument `context` is fed to this template.
"""
        # Save settings
        self._filename = filename
        self._flatten = flatten
        self._raise_warnings = raise_warnings

        self.raw_sql = self.readProcedure()

        # When we are forced to check for the procedures name, this already
        # gives us the argument-data needed to process the arguments, so save
        # this in case we need it later on
        argumentContent = None

        # Determine name of the procedure
        if name is None:
            argumentContent = self._generate_name()
        elif isinstance(name, str):
            self._name = name.decode('utf-8')
        elif isinstance(name, unicode):
            self._name = name
        else:
            raise InitializationException(
                    procedure   = self
                ,   field_name  = 'name'
                ,   field_types = (None, str, unicode)
                ,   field_value = name
            )

        # Determine the procedures arguments
        if arguments is None:
            self._generate_arguments(argumentContent)
        elif isinstance(arguments, list):
           self._generate_shuffle_arguments(arguments)
        else:
            raise InitializationException(
                    procedure   = self
                ,   field_name  = 'arguments'
                ,   field_types = (None, list)
                ,   field_value = arguments
            )

        # Determine whether the procedure should return any results
        if isinstance(results, bool):
            self._hasResults = results
        elif results is None:
            self._hasResults = False
        else:
            raise InitializationException(
                    procedure   = self
                ,   field_name  = 'results'
                ,   field_types = (None, bool)
                ,   field_value = results
            )

        # Determine additional context for the rendering of the procedure
        if isinstance(context, dict) or callable(context):
            self._context = context
        elif context is None:
            self._context = None
        else:
            raise InitializationException(
                    procedure   = self
                ,   field_name  = 'context'
                ,   field_types = (None, dict, 'function')
                ,   field_value = context
            )

        # Register the procedure
        registerProcedure(self)

    def readProcedure(self):
        """Read the procedure from the given location. The procedure is assumed to be stored in utf-8 encoding.

:raises: :exc:`~exceptions.FileDoesNotWorkException` in case the file could not be opened."""
        if hasattr(settings, 'IN_SITE_ROOT'):
            name = settings.IN_SITE_ROOT(self.filename)
        else:
            name = self.filename

        try:
            fileHandler = codecs.open(name, 'r', 'utf-8')
        except IOError as exp:
            raise FileDoesNotWorkException(
                procedure  = self,
                file_error = exp
            )

        return fileHandler.read()

    def renderProcedure(self, library):
        """Renders the stored procedure.

:param library: The library that contains the table information.
:raises: :exc:`~exceptions.ProcedureContextException` when the dynamic context's construction yields an :exc:`Exception`. When a reference to a table or column within the raw procedure does not exist, :exc:`~exceptions.ProcedureKeyException` is raised.

Whenever the context given on initialization is dynamic, it is computed here. First, the SQL will be treated as a django-template with as context the given context and 'name' set to the (escaped) name of the stored procedure. Next, references to tables and columns will be replaced. This depends on the library in use, which carries information about which tables exist. The default library in library almost always suffices."""
        # Determine context of the procedure
        renderContext = \
            {
                    'name'      :   connection.ops.quote_name(self.name)
            }

        # Fill in global context
        if not self._context is None:
            # fetch the context
            if callable(self._context):
                try:
                    context = self._context(self)
                except Exception as exp:
                    raise ProcedureContextException(
                            procedure = self
                        ,   exp       = exp
                    )
            else:
                context = self._context

            renderContext.update(context)

        # Render SQL
        sqlTemplate = Template(self.raw_sql)
        preprocessed_sql = sqlTemplate.render(Context(renderContext))

        # Fill in actual names
        self.sql = library.replaceNames(
                self.raw_sql
            ,   functools.partial(ProcedureKeyException, procedure = self)
        )

    def resetProcedure(self, library, verbosity = 2):
        """Renders the procedure and stores it in the database. See :meth:`~procedure.StoredProcedure.renderProcedure` and :meth:`~procedure.StoredProcedure.send_to_database` for details."""
        # Render the procedure
        self.renderProcedure(library)

        # Store the procedure in the database
        self.send_to_database(verbosity)

    def send_to_database(self, verbosity):
        """Store the stored procedure in the database.

:param verbosity: Determines how verbose we will be. On verbosity 2, warnings are printed to the standard output (default is 2)
:raises: :exc:`~exceptions.ProcedureCreationException` in case of database errors.

Note that we first try to delete the procedure, and then insert it. Take great care not to accidentally delete some other procedure which just happens to carry the same name, this is *not* prevented here.
"""
        cursor = connection.cursor()
        # Try to delete the procedure, if it exists
        try:
            # The database may give a warning when deleting a stored procedure which does not already
            # exist. This warning is worthless
            with warnings.catch_warnings(record = True) as ws:
                # When sufficiently verbose or pedantic, display warnings
                warnings.simplefilter('always' if verbosity >= 2 or self._raise_warnings else 'ignore')

                # Drop procedure if exists
                sql_check_proc = """
                    SELECT count(*)
                    FROM "SYS"."P_PROCEDURES_"
                    WHERE schema=current_schema
                    AND name='%s'
                """
                cursor.execute(sql_check_proc % (self.name.upper(),))
                if cursor.fetchone()[0] > 0:  # Procedure exists
                    cursor.execute('DROP PROCEDURE %s' % (self.name,))

                cursor.execute(self.sql)

                if len(ws) >= 1:
                    print "Warning during creation of %s" % self

                    for warning in ws:
                        print '\t%s' % warning.message

        except (DatabaseError, OperationalError) as exp:
            raise ProcedureCreationException(
                    procedure         = self
                ,   operational_error = exp
            )

        cursor.close()

    def __call__(self, *args, **kwargs):
        """Call the stored procedure. Arguments and keyword arguments to this method are fed to the stored procedure. First, all arguments are used, and then the keyword arguments are filled in.

:raises: Nameclashes result in a :exc:`TypeError`, invalid arguments yield :exc:`~exceptions.InvalidArgument` and too few arguments give rise to :exc:`~exceptions.InsufficientArguments`."""
        # Fetch the procedures arguments
        args = {arg.upper(): arg_value for arg, arg_value in zip(self.arguments, args)}

        cursor = connection.cursor()
        psid = cursor.prepare(self._call)
        ps = cursor.get_prepared_statement(psid)
        cursor.execute_prepared(ps, [args])

        # Always force the cursor to free its warnings
        with warnings.catch_warnings(record = True) as ws:
            warnings.simplefilter('always' if self._raise_warnings else 'ignore')

            results = []
            if self.hasResults:
                # There are some results to be fetched
                results.append(cursor.fetchall())
                while cursor.nextset() is not None:
                    results.append(cursor.fetchall())
            cursor.drop_prepared(psid)
            cursor.close()

            if len(ws) >= 1:
                # A warning was raised, raise it whenever the user wants
                raise ProcedureExecutionWarnings(
                        procedure   = self
                    ,   warnings    = ws
                )

        if self.hasResults:
            # if so requested, return only the first set of results
            if self._flatten and len(results) == 1 and len(results[0]) == 1:
                return results[0][0]
            else:
                return results

    # Properties
    name = property(
                fget = lambda self: self._name
            ,   doc  = 'Name of the stored procedure'
        )

    filename = property(
                fget = lambda self: self._filename
            ,   doc  = 'Filename of the stored procedure'
        )

    arguments = property(
                fget = lambda self: self._arguments
            ,   doc  = 'Arguments the procedure accepts'
        )

    hasResults = property(
                fget = lambda self: self._hasResults
            ,   doc  = 'Whether the stored procedures requires a fetch after execution'
        )

    call       = property(
                fget  = lambda self: self._call
            ,   doc   = 'The SQL code needed to call the stored procedure'
    )

    def _match_procedure(self):
        match = methodParser.match(self.raw_sql)

        if match is None:
            raise ProcedureNotParsableException(
                procedure = self
            )

        return match

    def _generate_name(self):
        match = self._match_procedure()

        self._name = match.group('name')

        return match.group('arguments')

    def _generate_arguments(self, argumentContent):
        # When the list of arguments is not given, we retrieve it from the procedure
        if argumentContent is None:
            argumentContent = self._match_procedure().group('arguments')

        # The data gathered in argumentData is not fully used now, only the name
        # is needed later on. In future versions, it might be useful to also use
        # the type information.
        argumentData = []

        for match in argumentParser.finditer(argumentContent):
            name  = match.group('name')
            type  = match.group('type')
            inout = match.group('inout')

            argumentData.append((name, type, inout))

        self._generate_shuffle_arguments(argumentData = argumentData)

    def _generate_shuffle_arguments(self, arguments = None, argumentData = None):
        """Generate a method for shuffling a dictionary whose keys match exactly the contents of arguments into the order as given by arguments."""
        # Generate the list of arguments. The user might have provided the arguments, in which case
        # we inspect arguments , otherwise we inferred them and use argumentData.
        arguments = self._arguments = [ name for (name, _, _) in argumentData] \
            if arguments is None else arguments
        argCount = len(arguments)

        # Generate the SQL needed to call the procedure
        self._generate_call(argCount)

        # Generate the function which shuffles the arguments into the appropriate
        # order on each stored procedure call
        def shuffle_argument(argValues):
            """Meant for internal use only, shuffles the arguments into correct order"""
            argumentValues = []
            givenArguments = set(argValues.keys())

            for argName in arguments:
                # Try to grab the argument
                try:
                    value = argValues.pop(argName)
                except KeyError:
                    continue

                argumentValues.append(value)

            # Notify the user of invalid arguments
            if len(argValues) > 0:
                raise InvalidArgument(
                            procedure = self
                        ,   arguments = argValues.keys()
                        ,   given     = givenArguments
                    )

            # Notify the user of missing arguments
            if len(argumentValues) < argCount:
                raise InsufficientArguments(
                        procedure          = self
                    ,   provided_arguments = argValues.keys()
                )

            return argumentValues

        self._shuffle_arguments = shuffle_argument

    def _generate_call(self, argCount):
        """Generates the call to the procedure"""
        self._call = 'CALL %s (%s)' % \
            (
                    self.name
                ,   ','.join('?' for _ in xrange(0, argCount))
            )

    def __unicode__(self):
        return u'%s (%s)' % (self.name, self.filename)

    def __str__(self):
        return unicode(self).encode('ascii', 'replace')

