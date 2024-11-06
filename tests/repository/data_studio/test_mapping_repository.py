from dataclasses import asdict
import unittest
from unittest.mock import MagicMock, Mock, patch, call
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
from  dacite import from_dict

from enums.data_studio import DataStudioMappingStatus
from model.data_studio import DataStudioMapping, DataStudioSaveMapping
from tests.test_utils import TestUtils
from repository import DataStudioMappingRepository
from exception import ServiceException
from enums import ServiceStatus
from utils import Singleton


class TestDataStudioMappingRepository(unittest.TestCase):


    TEST_RESOURCE_PATH = '/tests/resources/data_studio/'
    TEST_OWNER_ID = 'test_owner_id'
    TEST_USER_ID = 'test_user_id'
    TEST_MAPPING_ID = 'test_mapping_id'


    def setUp(self):
        self.app_config = Mock()
        self.aws_config = Mock()
        self.mock_table = Mock()
        Singleton.clear_instance(DataStudioMappingRepository)
        with patch('repository.data_studio.mapping_repository.DataStudioMappingRepository._DataStudioMappingRepository__configure_dynamodb') as mock_configure_dynamodb:
            self.mock_configure_dynamodb = mock_configure_dynamodb
            mock_configure_dynamodb.return_value = self.mock_table
            self.data_studio_mapping_repository = DataStudioMappingRepository(self.app_config, self.aws_config)


    def tearDown(self):
        self.app_config = None
        self.aws_config = None
        self.data_studio_mapping_repository = None
        self.mock_configure_dynamodb = None


    def test_get_active_mappings_success(self):
        """
        Test case for successfully retrieving active data studio mappings for a given owner.

        Expected Result: The method returns a list of active mappings associated with the owner.
        """
        mock_table_items_path = self.TEST_RESOURCE_PATH + "get_data_studio_mappings_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        self.mock_table.query.return_value = {'Items': mock_items}

        result = self.data_studio_mapping_repository.get_active_mappings(self.TEST_OWNER_ID)

        self.mock_table.query.assert_called_once_with(
            IndexName=self.app_config.data_studio_mappings_gsi_name,
            KeyConditionExpression=Key('owner_id').eq(self.TEST_OWNER_ID),
            FilterExpression=Attr('active').eq(True)
        )

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].active, True)
        self.assertEqual(result[1].active, True)


    def test_get_active_mappings_returns_empty_response_when_given_owner_id_does_not_have_active_or_inactive_mappings(self):
        """
        Test case for successfully retrieving empty data when owner does not have any mappings or any active mappings.

        Expected Result: The method returns an empty list mappings.
        """
        self.mock_table.query.return_value = {'Items': []}

        result = self.data_studio_mapping_repository.get_active_mappings(self.TEST_OWNER_ID)

        self.mock_table.query.assert_called_once_with(
            IndexName=self.app_config.data_studio_mappings_gsi_name,
            KeyConditionExpression=Key('owner_id').eq(self.TEST_OWNER_ID),
            FilterExpression=Attr('active').eq(True)
        )
        self.assertEqual(len(result), 0)


    def test_get_active_mappings_failure(self):
        """
        Test case for handling failure while retrieving active data studio mappings due to a ClientError.

        Expected Result: The method raises a ServiceException.
        """
        error_response = {
            'Error': {
                'Code': 'InternalServerError',
                'Message': 'An internal server error occurred'
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 500
            }
        }
        self.mock_table.query.side_effect = ClientError(error_response, 'query')

        with self.assertRaises(ServiceException) as context:
            self.data_studio_mapping_repository.get_active_mappings(self.TEST_OWNER_ID)

        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(str(context.exception.message), 'Failed to retrieve data studio mappings')

        self.mock_table.query.assert_called_once_with(
            IndexName=self.app_config.data_studio_mappings_gsi_name,
            KeyConditionExpression=Key('owner_id').eq(self.TEST_OWNER_ID),
            FilterExpression=Attr('active').eq(True)
        )


    def test_get_mapping_success(self):
        """
        Test case for successfully retrieving data studio mapping for a given owner & mapping.
        """
        mock_table_items_path = self.TEST_RESOURCE_PATH + "get_data_studio_mapping_response.json"
        mock_items = TestUtils.get_file_content(mock_table_items_path)

        self.mock_table.query.return_value = {'Items': mock_items}

        result = self.data_studio_mapping_repository.get_mapping(self.TEST_OWNER_ID, self.TEST_MAPPING_ID)

        # Assertion
        self.mock_table.query.assert_called_once_with(
            KeyConditionExpression=Key('id').eq(self.TEST_MAPPING_ID),
            FilterExpression=Attr('owner_id').eq(self.TEST_OWNER_ID)
        )
        self.assertEqual(len(result), 3)


    def test_get_mapping_return_empty_list_for_non_existing_mapping_id(self):
        """
        Test case for successfully retrieving empty list for non existing mapping id.
        """
        self.mock_table.query.return_value = {'Items': []}

        result = self.data_studio_mapping_repository.get_mapping(self.TEST_OWNER_ID, self.TEST_MAPPING_ID)

        # Assertion
        self.mock_table.query.assert_called_once_with(
            KeyConditionExpression=Key('id').eq(self.TEST_MAPPING_ID),
            FilterExpression=Attr('owner_id').eq(self.TEST_OWNER_ID)
        )
        self.assertEqual(len(result), 0)


    def test_get_mapping_failure(self):
        """
        Test case for handling failure while retrieving data studio mapping due to a ClientError.
        """
        error_response = {
            'Error': {
                'Code': 'InternalServerError',
                'Message': 'An internal server error occurred'
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 500
            }
        }
        self.mock_table.query.side_effect = ClientError(error_response, 'query')

        with self.assertRaises(ServiceException) as context:
            self.data_studio_mapping_repository.get_mapping(self.TEST_OWNER_ID, self.TEST_MAPPING_ID)

        # Assertion
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(str(context.exception.message), 'Failed to retrieve data studio mapping')

        self.mock_table.query.assert_called_once_with(
            KeyConditionExpression=Key('id').eq(self.TEST_MAPPING_ID),
            FilterExpression=Attr('owner_id').eq(self.TEST_OWNER_ID)
        )


    def test_create_mapping_success(self):
        """
        Test case for successfully creating a data studio mapping entry in database.

        Expected Result: The method should successfully call the put item of dynamodb table.
        """
        mapping = DataStudioMapping(
            id='mocked_mapping_id',
            revision=self.TEST_USER_ID,
            created_by=self.TEST_USER_ID,
            owner_id=self.TEST_OWNER_ID,
            status=DataStudioMappingStatus.DRAFT.value,
            active=True
        )
        self.mock_table.put_item = MagicMock()

        self.data_studio_mapping_repository.create_mapping(mapping)

        self.mock_table.put_item.assert_called_once_with(Item=asdict(mapping))


    def test_create_mapping_should_raise_exception_when_db_call_fails(self):
        """
        Test case for handling failure while creating a data studio mapping entry in database due to a ClientError.

        Expected Result: The method raises a ServiceException.
        """
        mapping = DataStudioMapping(
            id='mocked_mapping_id',
            revision=self.TEST_USER_ID,
            created_by=self.TEST_USER_ID,
            owner_id=self.TEST_OWNER_ID,
            status=DataStudioMappingStatus.DRAFT.value,
            active=True
        )

        error_response = {
            'Error': {
                'Code': 'ClientError',
                'Message': 'Invalid parameter'
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 400
            }
        }

        self.mock_table.put_item.side_effect = ClientError(error_response, 'put_item')

        with self.assertRaises(ServiceException) as context:
            self.data_studio_mapping_repository.create_mapping(mapping)

        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(str(context.exception.message), 'Couldn\'t create the mapping')


    def test_get_user_draft_success(self):
        """
        Test case for successfully retrieving user data studio mapping draft.
        """
        mock_table_item_path = self.TEST_RESOURCE_PATH + "get_data_studio_user_mapping_draft_response.json"
        mock_item = TestUtils.get_file_content(mock_table_item_path)

        self.mock_table.query.return_value = {'Items': [mock_item]}

        result = self.data_studio_mapping_repository.get_user_draft(self.TEST_OWNER_ID, self.TEST_MAPPING_ID, self.TEST_USER_ID)

        # Assertion
        self.mock_table.query.assert_called_once_with(
            KeyConditionExpression=Key('id').eq(self.TEST_MAPPING_ID) & Key('revision').eq(self.TEST_USER_ID),
            FilterExpression=Attr('owner_id').eq(self.TEST_OWNER_ID) & Attr('status').eq(DataStudioMappingStatus.DRAFT.value)
        )
        self.assertEqual(result, from_dict(DataStudioMapping, mock_item))


    def test_get_user_draft_without_user_draft_must_return_none(self):
        """
        Test case for retrieving none when user data studio mapping draft does not exist.
        """
        self.mock_table.query.return_value = {'Items': []}

        result = self.data_studio_mapping_repository.get_user_draft(self.TEST_OWNER_ID, self.TEST_MAPPING_ID, self.TEST_USER_ID)

        # Assertion
        self.mock_table.query.assert_called_once_with(
            KeyConditionExpression=Key('id').eq(self.TEST_MAPPING_ID) & Key('revision').eq(self.TEST_USER_ID),
            FilterExpression=Attr('owner_id').eq(self.TEST_OWNER_ID) & Attr('status').eq(DataStudioMappingStatus.DRAFT.value)
        )
        self.assertIsNone(result)


    def test_get_user_draft_failure(self):
        """
        Test case for handling failure while retrieving data studio user mapping draft due to a ClientError.
        """
        error_response = {
            'Error': {
                'Code': 'InternalServerError',
                'Message': 'An internal server error occurred'
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 500
            }
        }
        self.mock_table.query.side_effect = ClientError(error_response, 'query')

        with self.assertRaises(ServiceException) as context:
            self.data_studio_mapping_repository.get_user_draft(self.TEST_OWNER_ID, self.TEST_MAPPING_ID, self.TEST_USER_ID)

        # Assertion
        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(str(context.exception.message), 'Failed to retrieve user draft')

        self.mock_table.query.assert_called_once_with(
            KeyConditionExpression=Key('id').eq(self.TEST_MAPPING_ID) & Key('revision').eq(self.TEST_USER_ID),
            FilterExpression=Attr('owner_id').eq(self.TEST_OWNER_ID) & Attr('status').eq(DataStudioMappingStatus.DRAFT.value)
        )


    def test_save_mapping_success(self):
        """
        Test that save_mapping successfully updates the item in the database.
        """
        mock_table_item_path = self.TEST_RESOURCE_PATH + "get_data_studio_user_mapping_draft_response.json"
        mock_item = TestUtils.get_file_content(mock_table_item_path)

        mapping = from_dict(DataStudioSaveMapping, mock_item)
        self.mock_table.put_item = MagicMock()

        self.data_studio_mapping_repository.save_mapping(self.TEST_OWNER_ID, self.TEST_USER_ID, mapping)
        self.mock_table.put_item.assert_called_once_with(Item=asdict(mapping))


    def test_save_mapping_should_raise_exception_when_db_call_fails(self):
        """Test that save_mapping raises ServiceException on database update failure."""
        mock_table_item_path = self.TEST_RESOURCE_PATH + "get_data_studio_user_mapping_draft_response.json"
        mock_item = TestUtils.get_file_content(mock_table_item_path)
        mapping = from_dict(DataStudioSaveMapping, mock_item)

        error_response = {
            'Error': {
                'Code': 'InternalServerError',
                'Message': 'An internal server error occurred'
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 500
            }
        }
        self.mock_table.put_item.side_effect = ClientError(error_response, 'put_item')

        with self.assertRaises(ServiceException) as context:
            self.data_studio_mapping_repository.save_mapping(self.TEST_OWNER_ID, self.TEST_USER_ID, mapping)

        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(str(context.exception.message), 'Could not update the mapping draft')

        self.mock_table.put_item.assert_called_once_with(Item=asdict(mapping))


    def test_get_active_published_mapping_success(self):
        """
        Test case for successfully retrieving current active data studio published mapping.
        """
        mock_table_item_path = self.TEST_RESOURCE_PATH + "get_active_published_mapping_response.json"
        mock_item = TestUtils.get_file_content(mock_table_item_path)

        self.mock_table.query.return_value = {'Items': mock_item}

        result = self.data_studio_mapping_repository.get_active_published_mapping(self.TEST_OWNER_ID, self.TEST_MAPPING_ID)

        # Assertion
        self.mock_table.query.assert_called_once_with(
            KeyConditionExpression=Key('id').eq(self.TEST_MAPPING_ID),
            FilterExpression=Attr('owner_id').eq(self.TEST_OWNER_ID) & Attr('status').eq(DataStudioMappingStatus.PUBLISHED.value) & Attr('active').eq(True),
            ConsistentRead=True
        )
        self.assertEqual(result, from_dict(DataStudioMapping, mock_item[0]))


    def test_get_active_published_mapping_not_found(self):
        """
        Test case for retrieving none when current active data studio published mapping does not exist.
        """
        self.mock_table.query.return_value = {'Items': []}

        result = self.data_studio_mapping_repository.get_active_published_mapping(self.TEST_OWNER_ID, self.TEST_MAPPING_ID)

        # Assertion
        self.mock_table.query.assert_called_once_with(
            KeyConditionExpression=Key('id').eq(self.TEST_MAPPING_ID),
            FilterExpression=Attr('owner_id').eq(self.TEST_OWNER_ID) & Attr('status').eq(DataStudioMappingStatus.PUBLISHED.value) & Attr('active').eq(True),
            ConsistentRead=True
        )
        self.assertIsNone(result)


    def test_get_active_published_mapping_client_error(self):
        """
        Test case for handling failure while retrieving current active data studio published mapping due to a ClientError.
        """
        error_response = {
            'Error': {
                'Code': 'InternalServerError',
                'Message': 'An internal server error occurred'
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 500
            }
        }
        self.mock_table.query.side_effect = ClientError(error_response, 'query')

        # Act & Assert
        with self.assertRaises(ServiceException) as context:
            self.data_studio_mapping_repository.get_active_published_mapping(self.TEST_OWNER_ID, self.TEST_MAPPING_ID)

        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(str(context.exception.message), 'Failed to get active mapping')


    def test_publish_mapping_success(self):
        """
        Test that publish_mapping successfully updates and deletes the items in the database.
        """
        current_active_mapping = DataStudioMapping(
            id=self.TEST_MAPPING_ID,
            revision="1",
            active=False,
            owner_id=self.TEST_OWNER_ID,
            created_by=self.TEST_USER_ID,
        )

        new_mapping = DataStudioMapping(
            id=self.TEST_MAPPING_ID,
            revision="2",
            active=True,
            owner_id=self.TEST_OWNER_ID,
            created_by=self.TEST_USER_ID,
        )

        draft_mapping = DataStudioMapping(
            id=self.TEST_MAPPING_ID,
            revision=self.TEST_USER_ID,
            active=True,
            owner_id=self.TEST_OWNER_ID,
            created_by=self.TEST_USER_ID,
        )

         # Create a mock context manager
        mock_batch = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_batch)
        mock_context.__exit__ = MagicMock(return_value=None)

        self.mock_table.batch_writer = MagicMock(return_value=mock_context)

        # Act
        self.data_studio_mapping_repository.publish_mapping(
            new_mapping=new_mapping,
            current_active_mapping=current_active_mapping,
            draft_mapping=draft_mapping
        )

        # Assert
        # Verify batch_writer was called
        self.mock_table.batch_writer.assert_called_once()

        expected_calls = [
            call.put_item(Item=asdict(current_active_mapping)),
            call.put_item(Item=asdict(new_mapping)),
            call.delete_item(Key={'id': draft_mapping.id, 'revision': draft_mapping.revision})
        ]

        mock_batch.assert_has_calls(expected_calls, any_order=False)


    def test_publish_mapping_should_call_put_item_once_if_current_active_mapping_is_none(self):
        """
        Test that publish_mapping calls put_item only once if current_active_mapping is None.
        """
        new_mapping = DataStudioMapping(
            id=self.TEST_MAPPING_ID,
            revision="2",
            active=True,
            owner_id=self.TEST_OWNER_ID,
            created_by=self.TEST_USER_ID,
        )

        draft_mapping = DataStudioMapping(
            id=self.TEST_MAPPING_ID,
            revision=self.TEST_USER_ID,
            active=True,
            owner_id=self.TEST_OWNER_ID,
            created_by=self.TEST_USER_ID,
        )

         # Create a mock context manager
        mock_batch = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_batch)
        mock_context.__exit__ = MagicMock(return_value=None)

        self.mock_table.batch_writer = MagicMock(return_value=mock_context)

        # Act
        self.data_studio_mapping_repository.publish_mapping(
            new_mapping=new_mapping,
            current_active_mapping=None,
            draft_mapping=draft_mapping
        )

        # Assert
        # Verify batch_writer was called
        self.mock_table.batch_writer.assert_called_once()

        expected_calls = [
            call.put_item(Item=asdict(new_mapping)),
            call.delete_item(Key={'id': draft_mapping.id, 'revision': draft_mapping.revision})
        ]

        mock_batch.assert_has_calls(expected_calls, any_order=False)


    def test_publish_mapping_should_handle_client_error(self):
        """
        Test that publish_mapping raises service exception if ClientError occurs.
        """
        new_mapping = DataStudioMapping(
            id=self.TEST_MAPPING_ID,
            revision="2",
            active=True,
            owner_id=self.TEST_OWNER_ID,
            created_by=self.TEST_USER_ID,
        )

        draft_mapping = DataStudioMapping(
            id=self.TEST_MAPPING_ID,
            revision=self.TEST_USER_ID,
            active=True,
            owner_id=self.TEST_OWNER_ID,
            created_by=self.TEST_USER_ID,
        )

         # Create a mock context manager
        mock_batch = MagicMock()
        mock_context = MagicMock()
        mock_context.__enter__ = MagicMock(return_value=mock_batch)
        mock_context.__exit__ = MagicMock(return_value=None)

        self.mock_table.batch_writer = MagicMock(return_value=mock_context)

        error_response = {
            'Error': {
                'Code': 'InternalServerError',
                'Message': 'An internal server error occurred'
            },
            'ResponseMetadata': {
                'HTTPStatusCode': 500
            }
        }

        mock_batch.put_item.side_effect = [ClientError(error_response, 'put_item')]

        # Act
        with self.assertRaises(ServiceException) as context:
            self.data_studio_mapping_repository.publish_mapping(
                new_mapping=new_mapping,
                current_active_mapping=None,
                draft_mapping=draft_mapping
            )

        self.assertEqual(context.exception.status, ServiceStatus.FAILURE)
        self.assertEqual(context.exception.status_code, 500)
        self.assertEqual(str(context.exception.message), 'Failed to publish mapping')

        # Assert
        # Verify batch_writer was called
        self.mock_table.batch_writer.assert_called_once()

        expected_calls = [
            call.put_item(Item=asdict(new_mapping))
        ]

        mock_batch.assert_has_calls(expected_calls, any_order=False)