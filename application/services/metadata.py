import datetime
from nameko.rpc import rpc
from pymongo import ASCENDING
import bson.json_util
from nameko_mongodb.database import MongoDatabase
import sqlparse


class MetadataServiceError(Exception):
    pass


class MetadataService(object):
    name = 'metadata'

    database = MongoDatabase(result_backend=False)

    types = ['transform', 'predict', 'fit']

    @staticmethod
    def _check_function(_function):
        sqls = sqlparse.parse(_function)

        if len(sqls) != 1:
            return False

        if sqls[0].get_type() != 'CREATE':
            return False

        check_keywords = list(filter(lambda x: x.value in ('FUNCTION', 'LANGUAGE', 'PYTHON',), sqls[0].tokens))

        if len(check_keywords) != 3:
            return False

        return True

    @staticmethod
    def _check_query(query):
        sqls = sqlparse.parse(query)

        if len(sqls) != 1:
            return False

        if sqls[0].get_type() != 'SELECT':
            return False

        return True

    @staticmethod
    def _extract_function_name(_function):
        sqls = sqlparse.parse(_function)

        for token in sqls[0].tokens:
            if isinstance(token, sqlparse.sql.Identifier):
                return token.value

        return None

    @rpc
    def add_transformation(self, _id, _type, _function, _input=None, target_table=None, trigger_tables=None,
                           depends_on=None):
        self.database.transformations.create_index([('type', ASCENDING), ('trigger_tables', ASCENDING),
                                                    ('materialized', ASCENDING)])
        self.database.transformations.create_index('id', unique=True)

        function_only = False
        if _input is None:
            function_only = True

            if not (target_table is None and trigger_tables is None and depends_on is None):
                raise MetadataServiceError(
                    'Function only transformation can not have a not none target table, trigger tables or dependency')

        materialized = False
        if target_table is not None:
            materialized = True

        if _type not in self.types:
            raise MetadataServiceError('Unavailable types {}'.format(_type))

        if _input is not None and self._check_query(_input) is False:
            raise MetadataServiceError('Bad formatted query: {}'.format(_input))

        if self._check_function(_function) is False:
            raise MetadataServiceError('Bad formatted function: {}'.format(_function))

        function_name = self._extract_function_name(_function)

        if depends_on is not None and self.database.transformations.find_one({'id': depends_on}) is None:
            raise MetadataServiceError('Unknown dependency {}'.format(depends_on))

        self.database.transformations.update_one(
            {'id': _id},
            {
                '$set': {
                    'type': _type,
                    'function': _function,
                    'input': _input,
                    'target_table': target_table,
                    'trigger_tables': trigger_tables,
                    'depends_on': depends_on,
                    'materialized': materialized,
                    'function_only': function_only,
                    'function_name': function_name,
                    'creation_date': datetime.datetime.utcnow(),
                    'process_date': None
                }
            }, upsert=True
        )

        return {'id': _id}

    @rpc
    def delete_transformation(self, _id):
        if self.database.transformations.find_one({'depends_on': _id}) is not None:
            raise MetadataServiceError('At least one transformation depends on {}'.format(_id))

        self.database.transformations.delete_one({'id': _id})

        return {'id': _id}

    @rpc
    def update_process_date(self, _id):
        self.database.transformations.update_one({'id': _id}, {'$set': {'process_date': datetime.datetime.utcnow()}})

    @rpc
    def get_types(self):
        return self.types

    @rpc
    def get_update_pipeline(self, table):
        cursor = self.database.transformations.aggregate([
            {
                '$match': {
                    'trigger_tables': table
                }
            },
            {
                '$graphLookup': {
                    'from': 'transformations',
                    'startWith': '$depends_on',
                    'connectFromField': 'depends_on',
                    'connectToField': 'id',
                    'as': 'dependencies'
                }
            },
            {
                '$group': {
                    '_id': None,
                    'max_size': {'$max': {'$size': '$dependencies'}},
                    'transformations': {
                        '$push': {
                            'id': '$id',
                            'materialized': '$materialized',
                            'function_name': '$function_name',
                            'function': '$function',
                            'target_table': '$target_table',
                            'function_only': '$function_only',
                            'type': '$type',
                            'input': '$input',
                            'process_date': '$process_date',
                            'size': {'$size': '$dependencies'},
                            'dependencies': '$dependencies'
                        }
                    }
                }
            },
            {
                '$project': {
                    'transformations': {
                        '$filter': {
                            'input': '$transformations',
                            'as': 'transformations',
                            'cond': {'$eq': ['$$transformations.size', '$max_size']}
                        }
                    }
                }
            },
            {
                '$unwind': '$transformations'
            },
            {
                '$project': {
                    '_id': False
                }
            }
        ])

        result = list(cursor)

        if len(result) != 0:
            return bson.json_util.dumps(list(cursor)[0])

        return None
