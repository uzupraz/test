import unittest
from unittest.mock import MagicMock

from enums import DataStudioMappingStatus
from exception import ServiceException
from model import DataStudioMapping
from service import DataStudioMappingService
from enums import ServiceStatus


class TestDataStudioMappingService(unittest.TestCase):


    def setUp(self) -> None:
        self.mock_data_studio_mapping_repository = MagicMock()
        self.data_studio_mapping_service = DataStudioMappingService(self.mock_data_studio_mapping_repository)


    def tearDown(self) -> None:
        self.mock_data_studio_mapping_repository = None
        self.data_studio_mapping_service = None


    def test_get_active_mappings_success(self):
        """
        Test case for successfully retrieving active mappings.
        """
        owner_id = 'test_owner_id'

        mock_mappings = [
            DataStudioMapping(
                owner_id= owner_id,
                id= "map1",
                revision= "1",
                status= DataStudioMappingStatus.PUBLISHED.value,
                active= True,
                created_by= "creator1",
                name= "Mapping 1",
                description= "Test mapping 1",
                sources= {"source1": "data1"},
                output= {"output1": "result1"},
                mapping= {"map_field": "mapped_data"},
                published_by= "publisher1",
                published_at= 1633036800,
                version="1.0"
            )
        ]
        self.data_studio_mapping_service.data_studio_mapping_repository.get_active_mappings = MagicMock(return_value=mock_mappings)

        result = self.data_studio_mapping_service.get_active_mappings(owner_id)

        self.assertEqual(result, mock_mappings)


    def test_get_active_mappings_empty(self):
        """
        Test case for handling empty data from the repository.
        Expected Result: The service should return an empty list.
        """
        owner_id = 'test_owner_id'

        self.data_studio_mapping_service.data_studio_mapping_repository.get_active_mappings = MagicMock(return_value=[])

        result = self.data_studio_mapping_service.get_active_mappings(owner_id)

        self.assertEqual(result, [])


    def test_get_active_mappings_failure(self):
        """
        Test case for handling failure in the repository layer.
        Expected Result: ServiceException is raised by the service layer.
        """
        owner_id = 'test_owner_id'

        self.data_studio_mapping_service.data_studio_mapping_repository.get_active_mappings = MagicMock()
        self.data_studio_mapping_service.data_studio_mapping_repository.get_active_mappings.side_effect = ServiceException(
            status=ServiceStatus.FAILURE,
            status_code=500,
            message='Failed to retrieve data studio mappings'
        )

        with self.assertRaises(ServiceException) as context:
            self.data_studio_mapping_service.get_active_mappings(owner_id)

        self.assertEqual(context.exception.message, 'Failed to retrieve data studio mappings')
        self.assertEqual(context.exception.status_code, 500)