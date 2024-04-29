from dataclasses import dataclass


@dataclass
class User:
    sub: str
    organization_id: str

    @classmethod
    def from_authorizer_claims(cls, claims: dict) -> 'User':
        """
        Creates a new instance of the `User` class from the given authorizer claims.
        Args:
            claims (dict): A dictionary containing the authorizer claims.
        Returns:
            User: A new instance of the `User` class.
        """
        return cls(claims['sub'], claims['custom:organization_id'])


    def has_file_ownership(self, file_owner_id: str):
        """
        Checks if the requesting user has access for file related operations.

        Args:
            file_owner_id (str): The requesting user's owner ID.

        Returns:
            bool: True if the user has ownership, False otherwise.
        """
        return self.organization_id == file_owner_id