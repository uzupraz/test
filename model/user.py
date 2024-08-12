import json

from dataclasses import dataclass


@dataclass
class User:
    sub: str
    organization_id: str
    permissions: list


    @classmethod
    def from_authorizer_claims(cls, claims: dict) -> 'User':
        """
        Creates a new instance of the `User` class from the given authorizer claims.
        Args:
            claims (dict): A dictionary containing the authorizer claims.
        Returns:
            User: A new instance of the `User` class.
        """
        permissions = claims.get('custom:permissions', [])
        if isinstance(permissions, str):
            permissions = json.loads(permissions)

        return cls(claims['sub'], claims['custom:organizationId'], permissions)


    def has_file_ownership(self, file_owner_id: str):
        """
        Checks if the requesting user has access for file related operations.

        Args:
            file_owner_id (str): The requesting user's owner ID.

        Returns:
            bool: True if the user has ownership, False otherwise.
        """
        return self.organization_id == file_owner_id
    

    def has_permission(self, permission) -> bool:
        """
        Checks if the requesting user has requested permission or not.

        Args:
            permission (str): The requesting permission.

        Returns:
            bool: True if the user has permission, False otherwise.
        """
        expected_permission = f'{self.organization_id}:{permission}'
        generic_permission = f'{self.organization_id}:*'

        if expected_permission in self.permissions or generic_permission in self.permissions:
            return True
            
        return False