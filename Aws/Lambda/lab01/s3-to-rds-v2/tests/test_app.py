import pytest
from unittest.mock import patch, MagicMock
from app import process_file, save_to_db, handle_s3_file, transform_value
from app import handle_s3_file
from unittest.mock import patch
from app import process_file
from datetime import datetime
from app import transform_value

@pytest.fixture
def mock_config():
    return {
        'db': {
            'host': 'localhost',
            'user': 'test_user',
            'password': 'test_password',
            'database': 'test_db',
            'port': 3306
        },
        's3': {
            'bucket': 'dev-bucket-lab01',
            'input_prefix': 'entrada/',
            'processed_prefix': 'processados/',
            'error_prefix': 'erros/'
        },
        'batch_size': 1000,
        'mapping': {
            'file_delimiter': ';',
            'tables': [
                {
                    'name': 'tbv9088_regr_prod_plar',
                    'columns': [
                        {'name': 'cod_regr_prod_plar', 'source': {'type': 'column', 'index': 4}},
                        {'name': 'nom_regr_prod_plar', 'source': {'type': 'column', 'index': 7}},
                        {'name': 'ind_rgto_ativ', 'source': {'type': 'constant', 'value': 'S'}},
                        {'name': 'dat_hor_usua_atui_rgto', 'source': {'type': 'function', 'value': 'now'}},
                        {'name': 'dat_hor_inio_vige__regr_prod', 'source': {'type': 'column', 'index': 2, 'transform': 'date'}},
                        {'name': 'num_funl_cola_cogl_atud', 'source': {'type': '', 'value': '000000000'}},
                        {'name': 'num_funl_cola_cogl_atud', 'source': {1: '', 'value': '000000000'}},
                        {'name': 'des_regr_prod_plar', 'source': {'type': 'column', 'index': 7}}
                    ]
                }
            ]
        }
    }

def test_get_config(mock_config):
    assert 'db' in mock_config
    assert 's3' in mock_config
    assert mock_config['db']['port'] == 3306

def test_transform_value():
    assert transform_value('01.01.2023', 'date') == '2023-01-01 00:00:00'
    assert transform_value('C', 'ippi') == 1
    assert transform_value('X', 'ippi') == 2
    assert transform_value(None, 'now') is not None

@patch('app.csv.reader')
def test_process_file(mock_csv_reader, mock_config):
    mock_csv_reader.return_value = [
        ['value1', 'value2', '01.01.2023', 'C', 'value5', 'value6', 'value7']
    ]
    content = "value1;value2;01.01.2023;C;value5;value6;value7"
    table_data = process_file(content, mock_config)
    assert 'tbv9088_regr_prod_plar' in table_data
    assert len(table_data['tbv9088_regr_prod_plar']) == 1

@patch('app.mysql.connector.connect')
def test_save_to_db(mock_connect, mock_config):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_connect.return_value = mock_conn
    mock_conn.cursor.return_value = mock_cursor

    table_data = {
        'tbv9088_regr_prod_plar': [
            ('value1', 'value2', '2023-01-01 00:00:00', 'S', '2023-01-01 00:00:00', '2023-01-01 00:00:00', '000000000')
        ]
    }
    result = save_to_db(table_data, mock_config)
    assert result is True
    mock_cursor.executemany.assert_called()

@patch('app.boto3.client')
def test_handle_s3_file(mock_boto3_client, mock_config):
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    mock_s3.get_object.return_value = {
        'Body': MagicMock(read=lambda: b"value1;value2;01.01.2023;C;value5;value6;value7")
    }

    result = handle_s3_file('entrada/test.csv', mock_config)
    assert result['status'] == 'error'
    mock_s3.copy_object.assert_called()
    mock_s3.delete_object.assert_called()
@patch('app.csv.reader')
def test_process_file_valid_data(mock_csv_reader, mock_config):
    mock_csv_reader.return_value = [
        ['value1', 'value2', '01.01.2023', 'C', 'value5', 'value6', 'value7']
    ]
    content = "value1;value2;01.01.2023;C;value5;value6;value7"
    table_data = process_file(content, mock_config)
    assert 'tbv9088_regr_prod_plar' in table_data
    assert len(table_data['tbv9088_regr_prod_plar']) == 1
    assert table_data['tbv9088_regr_prod_plar'][0][0] == 'value5'  # Example column mapping

@patch('app.csv.reader')
def test_process_file_empty_row(mock_csv_reader, mock_config):
    mock_csv_reader.return_value = [
        ['value1', 'value2', '01.01.2023', 'C', 'value5', 'value6', 'value7'],
        []
    ]
    content = "value1;value2;01.01.2023;C;value5;value6;value7\n"
    table_data = process_file(content, mock_config)
    assert len(table_data['tbv9088_regr_prod_plar']) == 1  # Empty row should be ignored



@patch('app.csv.reader')
def test_process_file_multiple_rows(mock_csv_reader, mock_config):
    mock_csv_reader.return_value = [
        ['value1', 'value2', '01.01.2023', 'C', 'value5', 'value6', 'value7'],
        ['value8', 'value9', '02.02.2023', 'X', 'value10', 'value11', 'value12']
    ]
    content = "value1;value2;01.01.2023;C;value5;value6;value7\nvalue8;value9;02.02.2023;X;value10;value11;value12"
    table_data = process_file(content, mock_config)
    assert len(table_data['tbv9088_regr_prod_plar']) == 2
    assert table_data['tbv9088_regr_prod_plar'][1][0] == 'value10'  # Second row data


@patch('app.boto3.client')
@patch('app.process_file')
@patch('app.save_to_db')
def test_handle_s3_file_success(mock_save_to_db, mock_process_file, mock_boto3_client, mock_config):
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    mock_s3.get_object.return_value = {
        'Body': MagicMock(read=lambda: b"value1;value2;01.01.2023;C;value5;value6;value7")
    }
    mock_process_file.return_value = {
        'tbv9088_regr_prod_plar': [('value1', 'value2', '2023-01-01 00:00:00', 'S', '2023-01-01 00:00:00', '000000000')]
    }
    mock_save_to_db.return_value = True

    result = handle_s3_file('entrada/test.csv', mock_config)

    assert result['status'] == 'success'
    assert 'processed_tables' in result
    assert result['processed_tables']['tbv9088_regr_prod_plar'] == 1
    assert result['file'] == 'processados/test.csv'
    mock_s3.copy_object.assert_called()
    mock_s3.delete_object.assert_called()




@patch('app.boto3.client')
@patch('app.process_file')
@patch('app.save_to_db')
def test_handle_s3_file_processing_error(mock_save_to_db, mock_process_file, mock_boto3_client, mock_config):
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    mock_s3.get_object.return_value = {
        'Body': MagicMock(read=lambda: b"value1;value2;01.01.2023;C;value5;value6;value7")
    }
    mock_process_file.side_effect = Exception("Processing error")

    result = handle_s3_file('entrada/test.csv', mock_config)

    assert result['status'] == 'error'
    assert 'error' in result
    assert result['file'] == 'entrada/test.csv'
    mock_s3.copy_object.assert_called()
    mock_s3.delete_object.assert_called()




@patch('app.boto3.client')
def test_handle_s3_file_copy_object_exception(mock_boto3_client, mock_config):
    # Mock do cliente S3
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3

    # Simula o retorno do método get_object
    mock_s3.get_object.return_value = {
        'Body': MagicMock(read=lambda: b"value1;value2;01.01.2023;C;value5;value6;value7")
    }

    # Simula uma exceção no método copy_object
    mock_s3.copy_object.side_effect = Exception("Erro ao copiar arquivo para a pasta de erros")

    # Executa a função handle_s3_file
    result = handle_s3_file('entrada/test.csv', mock_config)

    # Verifica se o status é 'error'
    assert result['status'] == 'error'

    # Verifica se o método delete_object não foi chamado devido à falha no copy_object
    mock_s3.delete_object.assert_not_called()

    # Verifica se o erro foi registrado no log
    with patch('app.logging.error') as mock_logging_error:
        handle_s3_file('entrada/test.csv', mock_config)
        mock_logging_error.assert_any_call("Falha ao mover arquivo para erro: Erro ao copiar arquivo para a pasta de erros")




@patch('app.logging.warning')
def test_process_file_missing_column(mock_logging_warning, mock_config):
    content = 'value1;value2;01.01.2023;C\n'
    table_data = process_file(content, mock_config)
    assert len(table_data['tbv9088_regr_prod_plar']) == 1
    assert table_data['tbv9088_regr_prod_plar'][0][0] is None  # Missing column results in None
    mock_logging_warning.assert_any_call("Índice 4 fora do range na linha 1 - usando valor padrão")

    
def test_transform_value_date_valid():
    assert transform_value('01.01.2023', 'date') == '2023-01-01 00:00:00'

def test_transform_value_date_invalid():
    result = transform_value('invalid_date', 'date')
    assert result == datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def test_transform_value_ippi_c():
    assert transform_value('C', 'ippi') == 1

def test_transform_value_ippi_other():
    assert transform_value('X', 'ippi') == 2

def test_transform_value_now():
    result = transform_value(None, 'now')
    assert result == datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def test_transform_value_no_transform():
    assert transform_value('some_value', 'unknown') == 'some_value'












