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

    # with pytest.raises(MetadataServiceError):
    #     service.add_transformation(_id, _type, 'foo', job_id)

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


def test_get_all_transformations(database):
    service = worker_factory(MetadataService, database=database)
    result = bson.json_util.loads(service.get_all_transformations())
    assert len(result) == 0

    database.transformations.insert_one({
        'depends_on': None,
        'id': '0',
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
    result = bson.json_util.loads(service.get_all_transformations())
    assert len(result) == 1


def test_get_transformation(database):
    service = worker_factory(MetadataService, database=database)
    result = bson.json_util.loads(service.get_transformation('0'))
    assert not result

    database.transformations.insert_one({
        'depends_on': None,
        'id': '0',
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
    result = bson.json_util.loads(service.get_transformation('0'))
    assert result['id'] == '0'


def test_add_query(database):
    service = worker_factory(MetadataService, database=database)
    service.add_query('0', 'MyQuery', 'SELECT * FROM TOTO')

    doc = database.queries.find_one({'id': '0'})
    assert doc
    assert doc['creation_date']
    assert doc['id'] == '0'

    with pytest.raises(MetadataServiceError):
        service.add_query('0', 'MyQuery', 'foo')


def test_delete_query(database):
    service = worker_factory(MetadataService, database=database)
    database.queries.insert_one({
        'id': '0',
        'name': 'MyQuery',
        'sql': 'SELECT * FROM TOTO',
        'parameters': None
    })

    service.delete_query('0')
    assert not database.queries.find_one({'id': '0'})


def test_get_all_queries(database):
    service = worker_factory(MetadataService, database=database)
    database.queries.insert_one({
        'id': '0',
        'name': 'MyQuery',
        'sql': 'SELECT * FROM TOTO',
        'parameters': None
    })

    result = bson.json_util.loads(service.get_all_queries())
    assert len(result) == 1


def test_get_query(database):
    service = worker_factory(MetadataService, database=database)
    database.queries.insert_one({
        'id': '0',
        'name': 'MyQuery',
        'sql': 'SELECT * FROM TOTO',
        'parameters': None
    })

    result = bson.json_util.loads(service.get_query('0'))
    assert result['id'] == '0'


def test_add_template(database):
    service = worker_factory(MetadataService, database=database)
    service.add_template('0', 'MyTemplate', 'FR', 'ctx')

    doc = database.templates.find_one({'id': '0'})
    assert doc
    assert doc['creation_date']
    assert doc['id'] == '0'


def test_delete_template(database):
    service = worker_factory(MetadataService, database=database)
    database.templates.insert_one({
        'id': '0',
        'name': 'MyQuery',
        'language': 'FR',
        'context': 'ctx'
    })

    service.delete_template('0')
    assert not database.templates.find_one({'id': '0'})


def test_get_all_templates(database):
    service = worker_factory(MetadataService, database=database)
    database.templates.insert_one({
        'id': '0',
        'name': 'MyQuery',
        'language': 'FR',
        'context': 'ctx'
    })

    result = bson.json_util.loads(service.get_all_templates())
    assert len(result) == 1


def test_get_template(database):
    service = worker_factory(MetadataService, database=database)
    database.templates.insert_one({
        'id': '0',
        'name': 'MyQuery',
        'language': 'FR',
        'context': 'ctx'
    })

    result = bson.json_util.loads(service.get_template('0'))
    assert result['id'] == '0'


def test_add_query_to_template(database):
    service = worker_factory(MetadataService, database=database)
    database.templates.insert_one({
        'id': '0',
        'name': 'MyQuery',
        'language': 'FR',
        'context': 'ctx'
    })

    database.queries.insert_one({
        'id': '0',
        'name': 'MyQuery',
        'sql': 'SELECT * FROM TOTO WHERE TITI = %s',
        'parameters': ['titi']
    })

    service.add_query_to_template('0', '0', referential_parameters=[{'titi': 'toto'}])
    res = database.templates.find_one({'id': '0'})
    assert res['queries']
    assert res['queries'][0]['id'] == '0'

    service.add_query_to_template('0', '0', referential_parameters=[{'titi': 'toto'}], labels={'col': 'entity'})
    res = database.templates.find_one({'id': '0'})
    assert len(res['queries']) == 1
    assert res['queries'][0]['labels']

    with pytest.raises(MetadataServiceError):
        service.add_query_to_template('0', '1', referential_parameters=[{'titi': 'toto'}])

    with pytest.raises(MetadataServiceError):
        service.add_query_to_template('0', '0', referential_parameters=[{'tutu': 'toto'}])


def test_delete_query_from_template(database):
    service = worker_factory(MetadataService, database=database)
    database.templates.insert_one({
        'id': '0',
        'name': 'MyQuery',
        'language': 'FR',
        'context': 'ctx',
        'queries': [{'id': '0'}]
    })
    service.delete_query_from_template('0', '0')
    res = database.templates.find_one({'id': '0'})
    assert len(res['queries']) == 0


def test_update_svg_in_template(database):
    service = worker_factory(MetadataService, database=database)
    database.templates.insert_one({
        'id': '0',
        'name': 'MyQuery',
        'language': 'FR',
        'context': 'ctx'
    })
    service.update_svg_in_template('0', '<svg>toto</svg>')
    res = database.templates.find_one({'id': '0'})
    assert res['svg'] == '<svg>toto</svg>'
