import datetime
import re
import logging
from nameko.rpc import rpc
from nameko.events import event_handler
import bson.json_util
from nameko_mongodb.database import MongoDatabase
from pymongo import ASCENDING
import sqlparse

_logger = logging.getLogger(__name__)


class MetadataServiceError(Exception):
    pass


class MetadataService(object):
    name = 'metadata'

    database = MongoDatabase(result_backend=False)

    TYPES = ['transform', 'predict', 'fit']

    def _delete_outdated_subscriptions(self, user, meta_type, new_sub, old_sub):
        old = set()
        if 'subscription' in old_sub and meta_type in old_sub['subscription']:
            old = set(r for r in old_sub['subscription'][meta_type])

        new = set()
        if meta_type in new_sub:
            new = set(new_sub[meta_type])

        diff = old - new
        self.database[meta_type].update_many(
            {'id': {'$in': list(diff)}},
            {'$pull': {'allowed_users': user}}
        )

    def _add_subscriptions(self, user, meta_type, sub):
        if meta_type in sub:
            self.database[meta_type].update_many(
                {'id': {'$in': sub[meta_type]}},
                {'$addToSet': {'allowed_users': user}}
            )

    @event_handler('subscription_manager', 'user_sub')
    def handle_suscription(self, payload):
        user = payload['user']
        _logger.info('Receiving subscription for user {}'.format(user))
        if 'metadata' in payload['subscription']:
            metadata = payload['subscription']['metadata']
            old_sub = self.database.subscriptions.find_one(
                {'user': user}, {'subscription'})
            for t in ('templates',):
                _logger.info(
                    'Handling subscription for metadata type {}'.format(t))
                if old_sub:
                    self._delete_outdated_subscriptions(
                        user, t, metadata, old_sub)
                self._add_subscriptions(user, t, metadata)
            self.database.subscriptions.update_one({'user': user},
                                                   {'$set': {'subscription': metadata}}, upsert=True)

    @staticmethod
    def _check_function(_function):
        sqls = sqlparse.parse(_function)

        if len(sqls) != 1:
            return False

        if sqls[0].get_type() != 'CREATE':
            return False

        check_keywords = list(filter(lambda x: x.value in (
            'FUNCTION', 'LANGUAGE', 'PYTHON',), sqls[0].tokens))

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
        regex = re.search(r'CREATE FUNCTION ([A-Za-z_]+)', _function)

        if regex is not None:
            return regex.group(1)

        return None

    @staticmethod
    def _build_output(_input, function_name):
        return 'SELECT * FROM {}(({}))'.format(function_name, _input)

    @rpc
    def add_transformation(self, _id, _type, _function, job_id, _input=None, target_table=None, trigger_tables=None,
                           depends_on=None, parameters=None):
        self.database.transformations.create_index('trigger_tables')
        self.database.transformations.create_index('id', unique=True)

        function_only = False
        if _input is None:
            function_only = True

            if not (target_table is None and trigger_tables is None and depends_on is None):
                raise MetadataServiceError(
                    'Function only transformation can not have a not none target table, trigger tables or dependency')

        function_name = self._extract_function_name(_function)

        materialized = False
        if target_table is not None:
            materialized = True

        if _type not in self.TYPES:
            raise MetadataServiceError('Unavailable types {}'.format(_type))

        if _input is not None and self._check_query(_input) is False:
            raise MetadataServiceError(
                'Bad formatted query: {}'.format(_input))

        # if self._check_function(_function) is False:
        #     raise MetadataServiceError('Bad formatted function: {}'.format(_function))

        if depends_on is not None \
                and self.database.transformations.find_one({'id': depends_on, 'job_id': job_id}) is None:
            raise MetadataServiceError(
                'Unknown dependency {} for job_id {}'.format(depends_on, job_id))

        output = None
        if materialized is True:
            output = self._build_output(_input, function_name)

        self.database.transformations.update_one(
            {'id': _id},
            {
                '$set': {
                    'type': _type,
                    'function': _function,
                    'job_id': job_id,
                    'input': _input,
                    'parameters': parameters,
                    'output': output,
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
            raise MetadataServiceError(
                'At least one transformation depends on {}'.format(_id))

        self.database.transformations.delete_one({'id': _id})

        return {'id': _id}

    @rpc
    def update_process_date(self, _id):
        self.database.transformations.update_one(
            {'id': _id}, {'$set': {'process_date': datetime.datetime.utcnow()}})

    @rpc
    def get_types(self):
        return self.TYPES

    @rpc
    def get_all_transformations(self):
        cursor = self.database.transformations.find({}, {'_id': 0, 'input': 0, 'function': 0, 'output': 0, 'parameters': 0,
                                                         })

        return bson.json_util.dumps(list(cursor))

    @rpc
    def get_transformation(self, _id):
        return bson.json_util.dumps(self.database.transformations.find_one({'id': _id}, {'_id': 0}))

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
                    '_id': '$job_id',
                    'transformations': {
                        '$addToSet': {
                            'id': '$id',
                            'materialized': '$materialized',
                            'function_name': '$function_name',
                            'function': '$function',
                            'target_table': '$target_table',
                            'function_only': '$function_only',
                            'type': '$type',
                            'input': '$input',
                            'output': '$output',
                            'parameters': '$parameters',
                            'process_date': '$process_date',
                            'index': {'$size': '$dependencies'},
                            'dependencies': '$dependencies'
                        }
                    }
                }
            },
            {
                '$unwind': '$transformations'
            },
            {
                '$sort': {'transformations.index': 1}
            },
            {
                '$group': {
                    '_id': '$_id',
                    'transformations': {
                        '$push': {
                            'transformations': '$transformations'
                        }
                    }
                }
            },
            {
                '$project': {
                    'job_id': '$_id',
                    'transformations': '$transformations.transformations'
                }
            }
        ])

        result = list(cursor)

        if len(result) != 0:
            return bson.json_util.dumps(result)

        return None

    @rpc
    def add_query(self, _id, name, sql, parameters=None):
        self.database.queries.create_index('id', unique=True)

        if self._check_query(sql) is False:
            raise MetadataServiceError(
                'An error occured while parsing SQL query: {}'.format(sql))

        self.database.queries.update_one({'id': _id}, {
            '$set': {
                'name': name,
                'sql': sql,
                'parameters': parameters,
                'creation_date': datetime.datetime.utcnow()
            }
        }, upsert=True)

        return {'id': _id}

    @rpc
    def delete_query(self, _id):
        t = self.database.templates.find_one({'queries.id': _id})

        if t is not None:
            raise MetadataServiceError(
                'Template {} depends on query {}. Cannot delete it'.format(t['id'], _id))

        self.database.queries.delete_one({'id': _id})

        return {'id': _id}

    @rpc
    def get_all_queries(self):
        cursor = self.database.queries.find(
            {}, {'_id': 0, 'sql': 0, 'parameters': 0})

        return bson.json_util.dumps(list(cursor))

    @rpc
    def get_query(self, _id):
        return bson.json_util.dumps(self.database.queries.find_one({'id': _id}, {'_id': 0}))

    @rpc
    def add_template(self, _id, name, language, context, bundle, picture=None, kind='image', datasource=None):
        self.database.templates.create_index(
            [('id', ASCENDING), ('allowed_users', ASCENDING)])
        self.database.templates.create_index('id')
        self.database.templates.create_index('bundle')

        self.database.templates.update_one({'id': _id}, {
            '$set': {
                'name': name,
                'language': language,
                'context': context,
                'bundle': bundle,
                'creation_date': datetime.datetime.utcnow(),
                'picture': picture,
                'kind': kind,
                'datasource': datasource
            }
        }, upsert=True)

        return {'id': _id}

    @rpc
    def delete_template(self, _id):
        t = self.database.triggers.find_one({'template.id': _id})
        if t is not None:
            raise MetadataServiceError(
                'Trigger {} depends on template {}. Cannot delete it'.format(t['id'], _id))
        self.database.templates.delete_one({'id': _id})

        return {'id': _id}

    @rpc
    def get_all_templates(self, user):
        cursor = self.database.templates.find({'allowed_users': user},
                                              {'_id': 0, 'svg': 0, 'queries': 0})\
                                              .sort('id', ASCENDING)

        return bson.json_util.dumps(list(cursor))

    @rpc
    def get_templates_by_bundle(self, bundle, user):
        cursor = self.database.templates.find({'bundle': bundle, 'allowed_users': user},
                                              {'_id': 0, 'svg': 0, 'queries': 0})\
                                              .sort('id', ASCENDING)

        return bson.json_util.dumps(list(cursor))

    @rpc
    def get_template(self, _id, user):
        return bson.json_util.dumps(self.database.templates.find_one({'id': _id, 'allowed_users': user},
                                                                     {'_id': 0}))

    @rpc
    def add_query_to_template(self, _id, query_id, referential_parameters=None, labels=None, referential_results=None,
                              user_parameters=None, limit=50):
        template = self.database.templates.find_one({'id': _id})

        if template is None:
            raise MetadataServiceError('Template {} not found'.format(_id))

        query = self.database.queries.find_one({'id': query_id})

        if query is None:
            raise MetadataServiceError('Query {} not found'.format(query_id))

        if referential_parameters is not None:
            query_parameters = query['parameters']
            if query_parameters is None:
                raise MetadataServiceError(
                    'Query {} does not have parameters'.format(query_id))
            check_ref_params = [list(r.keys())[0] for r in referential_parameters if
                                list(r.keys())[0] in query_parameters]

            if len(check_ref_params) != len(referential_parameters):
                raise MetadataServiceError('Some referential parameters mismatching query {} parameters'
                                           .format(query_id))

        res = self.database.templates.update_one(
            {
                'id': _id,
                'queries': {
                    '$not': {
                        '$elemMatch': {
                            'id': query_id
                        }
                    }
                }
            },
            {
                '$addToSet': {
                    'queries': {
                        'id': query_id,
                        'referential_parameters': referential_parameters,
                        'labels': labels,
                        'referential_results': referential_results,
                        'user_parameters': user_parameters,
                        'limit': limit
                    }
                }
            }
        )

        if res.modified_count == 0:
            res = self.database.templates.update_one(
                {'id': _id, 'queries.id': query_id},
                {
                    '$set': {
                        'queries.$.referential_parameters': referential_parameters,
                        'queries.$.labels': labels,
                        'queries.$.referential_results': referential_results,
                        'queries.$.user_parameters': user_parameters,
                        'limit': limit
                    }
                }
            )

    @rpc
    def delete_query_from_template(self, _id, query_id):
        result = self.database.templates.update_one(
            {'id': _id},
            {
                '$pull': {'queries': {'id': query_id}}
            }
        )
        if result.modified_count == 0:
            raise MetadataServiceError('Nothing has been deleted')

    @rpc
    def update_svg_in_template(self, _id, svg):
        result = self.database.templates.update_one(
            {'id': _id, 'kind': 'image'},
            {
                '$set': {
                    'svg': svg
                }
            }
        )
        if result.modified_count == 0:
            raise MetadataServiceError('Nothing has been updated')

    @rpc
    def update_html_in_template(self, _id, html):
        result = self.database.templates.update_one(
            {'id': _id, 'kind': 'widget'},
            {
                '$set': {
                    'html': html
                }
            }
        )
        if result.modified_count == 0:
            raise MetadataServiceError('Nothing has been updated')

    @rpc
    def add_trigger(self, _id, name, on_event, template, selector=[], export=None):
        self.database.triggers.create_index('id', unique=True)
        self.database.triggers.create_index('on_event')

        if 'id' not in template:
            raise MetadataServiceError('ID not found in template spec')

        check = self.database.templates.find_one({'id': template['id']})

        if not check:
            raise MetadataServiceError(
                'Template {} not found'.format(template['id']))

        self.database.triggers.update_one({'id': _id}, {
            '$set': {
                'name': name,
                'on_event': on_event,
                'template': template,
                'selector': selector,
                'export': export
            }
        }, upsert=True)

        return {'id': _id}

    @rpc
    def delete_trigger(self, _id):
        self.database.triggers.delete_one({'id': _id})

        return {'id': _id}

    @rpc
    def get_trigger(self, _id):
        trigger = self.database.triggers.find_one({'id': _id}, {'_id': 0})
        return bson.json_util.dumps(trigger)

    @rpc
    def get_all_triggers(self):
        cursor = self.database.triggers.find({}, {'_id': 0})
        return bson.json_util.dumps(list(cursor))

    @rpc
    def get_fired_triggers(self, event_type):
        cursor = self.database.triggers.find(
            {'on_event': event_type}, {'_id': 0})
        return bson.json_util.dumps(list(cursor))
