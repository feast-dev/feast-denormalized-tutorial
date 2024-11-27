from dataclasses import dataclass
from datetime import datetime
from random import choice, random
from typing import Callable, List


@dataclass
class LoginAttempt:
    timestamp: datetime
    user_id: str
    ip_address: str
    success: bool

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp.isoformat(),
            "user_id": self.user_id,
            "ip_address": self.ip_address,
            "success": self.success,
        }


def create_login_attempt_generator(
    num_users: int, num_ips: int
) -> Callable[[], LoginAttempt]:
    if num_ips > num_users:
        raise ValueError("Number of IPs must be less than or equal to number of users")

    # Generate user IDs like "user_001", "user_002", etc.
    user_ids: List[str] = [f"user_{str(i).zfill(3)}" for i in range(num_users)]

    ip_addresses: List[str] = []
    for i in range(num_ips):
        octet3, octet4 = divmod(i, 255)
        ip_addresses.append(f"192.168.{octet3}.{octet4 + 1}")

    def generate_attempt() -> LoginAttempt:
        """Creates a random LoginAttempt instance."""
        return LoginAttempt(
            timestamp=datetime.now(),
            user_id=choice(user_ids),
            ip_address=choice(ip_addresses),
            success=random() > 0.2,
        )

    return generate_attempt
