import datetime
import pytest
from pymongo import MongoClient
import bson.json_util
from nameko.testing.services import worker_factory
from application.services.metadata import MetadataService, MetadataServiceError


@pytest.fixture
def database(db_url):
    client = MongoClient(db_url)

    yield client['test_db']

    client.drop_database('test_db')
    client.close()


def test_add_transformation(database):
    service = worker_factory(MetadataService, database=database)

    _id = '0'
    _type = 'transform'
    job_id = 'myjob'
    _function = '''
    CREATE FUNCTION my_function (data DOUBLE) RETURN TABLE (result DOUBLE) LANGUAGE PYTHON
    {

    }
    '''

    service.add_transformation(_id, _type, _function, job_id)
    trans = database.transformations.find_one({'id': _id})
    assert trans['id'] == _id
    assert trans['materialized'] is False
    assert trans['function_only'] is True
    assert trans['function_name'] == 'my_function'

    with pytest.raises(MetadataServiceError):
        service.add_transformation(_id, _type, 'foo', job_id)

    with pytest.raises(MetadataServiceError):
        service.add_transformation(_id, 'foo', _function, job_id)

    with pytest.raises(MetadataServiceError):
        service.add_transformation(_id, _type, _function, job_id, depends_on='other')

    with pytest.raises(MetadataServiceError):
        service.add_transformation(_id, _type, _function, job_id, target_table='table')

    _input = '''
    SELECT * FROM MYSOURCE
    '''

    trigger_tables = ['MYSOURCE']

    target_table = 'MYTARGET'

    service.add_transformation(_id, _type, _function, job_id, _input=_input, target_table=target_table,
                               trigger_tables=trigger_tables)

    trans = database.transformations.find_one({'id': _id})
    assert trans['materialized'] is True
    assert trans['function_only'] is False
    assert trans['output']

    with pytest.raises(MetadataServiceError):
        service.add_transformation(_id, _type, _function, job_id, _input='bar', target_table=target_table,
                                   trigger_tables=trigger_tables)

    service.add_transformation(_id, _type, _function, job_id, _input=_input, target_table=target_table,
                               trigger_tables=trigger_tables, depends_on=_id)
    trans = list(database.transformations.find({'depends_on': _id}))
    assert len(trans) == 1


def test_delete_transformation(database):
    service = worker_factory(MetadataService, database=database)

    _id = '0'

    database.transformations.insert_one({
        'depends_on': None,
        'id': _id,
        'materialized': False,
        'function_name': 'my_function',
        'function': 'CREATE FUNCTION my_function (data DOUBLE) RETURN TABLE (result DOUBLE) LANGUAGE PYTHON{}',
        'target_table': None,
        'function_only': True,
        'trigger_tables': None,
        'type': 'transform', 'input': None
    })

    service.delete_transformation(_id)

    assert not database.transformations.find_one({'id': _id})

    database.transformations.insert_many([
        {
            'depends_on': None,
            'id': _id,
            'materialized': False,
            'function_name': 'my_function',
            'function': 'CREATE FUNCTION my_function (data DOUBLE) RETURN TABLE (result DOUBLE) LANGUAGE PYTHON{}',
            'target_table': None,
            'function_only': True,
            'trigger_tables': None,
            'type': 'transform', 'input': None
        },
        {
            'depends_on': _id,
            'id': '1',
            'materialized': False,
            'function_name': 'my_function',
            'function': 'CREATE FUNCTION my_function (data DOUBLE) RETURN TABLE (result DOUBLE) LANGUAGE PYTHON{}',
            'target_table': None,
            'function_only': True,
            'trigger_tables': None,
            'type': 'transform', 'input': None
        }
    ])

    with pytest.raises(MetadataServiceError):
        service.delete_transformation(_id)


def test_update_process_date(database):
    service = worker_factory(MetadataService, database=database)

    _id = '0'

    database.transformations.insert_one({
        'depends_on': None,
        'id': _id,
        'materialized': False,
        'function_name': 'my_function',
        'function': 'CREATE FUNCTION my_function (data DOUBLE) RETURN TABLE (result DOUBLE) LANGUAGE PYTHON{}',
        'target_table': None,
        'function_only': True,
        'trigger_tables': None,
        'type': 'transform', 'input': None,
        'creation_date': datetime.datetime.utcnow(),
        'process_date': None
    })

    service.update_process_date(_id)

    trans = database.transformations.find_one({'id': _id})
    assert trans['process_date']


def test_get_update_pipeline(database):
    service = worker_factory(MetadataService, database=database)

    database.transformations.insert_many([
        {
            'depends_on': None,
            'id': '0',
            'materialized': False,
            'function_name': 'my_function',
            'function': 'CREATE FUNCTION my_function (data DOUBLE) RETURN TABLE (result DOUBLE) LANGUAGE PYTHON{}',
            'target_table': 'mytable',
            'function_only': True,
            'trigger_tables': ['source'],
            'type': 'transform',
            'job_id': 'myjob',
            'input': None
        },
        {
            'depends_on': '0',
            'id': '1',
            'materialized': False,
            'function_name': 'my_function',
            'function': 'CREATE FUNCTION my_function (data DOUBLE) RETURN TABLE (result DOUBLE) LANGUAGE PYTHON{}',
            'target_table': 'mytable',
            'function_only': True,
            'trigger_tables': ['source'],
            'type': 'transform',
            'job_id': 'myjob',
            'input': None
        },
        {
            'depends_on': '1',
            'id': '2',
            'materialized': False,
            'function_name': 'my_function',
            'function': 'CREATE FUNCTION my_function (data DOUBLE) RETURN TABLE (result DOUBLE) LANGUAGE PYTHON{}',
            'target_table': 'mytable',
            'function_only': True,
            'trigger_tables': ['source'],
            'type': 'transform',
            'job_id': 'myjob',
            'input': None
        },
        {
            'depends_on': '2',
            'id': '3',
            'materialized': False,
            'function_name': 'my_function',
            'function': 'CREATE FUNCTION my_function (data DOUBLE) RETURN TABLE (result DOUBLE) LANGUAGE PYTHON{}',
            'target_table': 'mytable',
            'function_only': True,
            'trigger_tables': ['source'],
            'type': 'transform',
            'job_id': 'myjob',
            'input': None
        },
        {
            'depends_on': '3',
            'id': '4',
            'materialized': False,
            'function_name': 'my_function',
            'function': 'CREATE FUNCTION my_function (data DOUBLE) RETURN TABLE (result DOUBLE) LANGUAGE PYTHON{}',
            'target_table': 'mytable',
            'function_only': True,
            'trigger_tables': ['source'],
            'type': 'transform',
            'job_id': 'myjob',
            'input': None
        },
        {
            'depends_on': None,
            'id': '5',
            'materialized': False,
            'function_name': 'my_function',
            'function': 'CREATE FUNCTION my_function (data DOUBLE) RETURN TABLE (result DOUBLE) LANGUAGE PYTHON{}',
            'target_table': 'mytable',
            'function_only': True,
            'trigger_tables': ['source'],
            'type': 'transform',
            'job_id': 'myjob2',
            'input': None
        }
    ])

    pipeline = service.get_update_pipeline('source')
    pipeline = bson.json_util.loads(pipeline)

    assert len(pipeline) == 2

    for p in pipeline:
        if p['_id'] == 'myjob':
            assert len(p['transformations']) == 5
        else:
            assert len(p['transformations']) == 1
