import ast

from dagster_university.models.address import Address
from dagster_university.models.agency import Agency
from dagster_university.models.agent import Agent


def find_address(addresses_list: list[Address], address_id: str):
    """
    Check if an address is present in the list.
    Args:
        addresses_list:
        address_id:
    Returns:
        Address: The address object if it exists.
        None: If the address is not present in the list.
    """
    for address in addresses_list:
        if address.address_id == address_id:
            return address
    return None


def find_agent(agents_list: list[Agent], email: str):
    """
    Check if an agent is present in the list.

    Args:
        agents_list:
        email:

    Returns:
        Agent: The agent object if it exists.
        None: If the agent is not present in the list.
    """
    for agent in agents_list:
        if agent.email == email:
            return agent
    return None


def find_agency(agencies_list: list[Agency], agency_id: int):
    """
    Check if an agency is present in the list.

    Args:
        agencies_list:
        agency_id:

    Returns:
        Agency: The agency object if it exists.
        None: If the agency is not present in the list.
    """

    for agency in agencies_list:
        if agency.agency_id == agency_id:
            return agency
    return None


THOUSAND = 1_000


def get_ad_price(price: str):
    try:
        if price is None or price == "":
            return 0, 0
        price = price.replace('"', '').replace(",", "").replace("$", "").replace(" ", "").lower()
        prices = price.split("_")
        prices = [int(p.replace("k", "")) * THOUSAND if p.endswith("k") else (
            int(p.replace("m", "")) * THOUSAND * 1000 if p.endswith("m") else int(p)) for p in prices]
        if len(prices) > 1:
            return prices[0], prices[1]
        else:
            return prices[0], prices[0]
    except Exception as e:
        return 0, 0


def get_rental_ad_price(price: str):
    try:
        if price is None or price == "":
            return 0, 0
        price = price.replace('"', '').replace(",", "").replace("$", "").replace(" ", "").lower()
        prices = price.split("_")
        prices = [int(p.replace("k", "")) * THOUSAND if p.endswith("k") else (
            int(p.replace("m", "")) * THOUSAND * 1000 if p.endswith("m") else int(p)) for p in prices]
        if len(prices) > 1:
            return prices[0], prices[1]
        else:
            return prices[0], prices[0]
    except Exception as e:
        return 0, 0


def get_surface_area(sa_str: str) -> int:
    try:
        if sa_str is None or sa_str == "":
            return 0
        sa_str = sa_str.replace('"', '').strip()
        if sa_str.endswith("m2"):
            return int(sa_str.replace("m2", "").strip())
        elif sa_str.endswith('ac'):
            return int(int(sa_str.replace('ac', '').strip()) * 4046.86)
        elif sa_str.endswith("ha"):
            return int(sa_str.replace("ha", "").strip()) * 10000
        else:
            return 0
    except Exception as e:
        return 0


def get_features(listing_id: str, feature_txt: str):
    print(f"Feature txt: {feature_txt}")
    if feature_txt is None or feature_txt == "":
        return []
    feature_txt = feature_txt.replace('"', '')
    features = ast.literal_eval(feature_txt)
    return [{"listing_id": listing_id, "feature": str(feature).strip()} for feature in features]


def get_agents(listing_id: str, agents_txt: str):
    if agents_txt is None or agents_txt == "":
        return []
    agents_txt = agents_txt.replace('"', '').strip()
    agents = ast.literal_eval(agents_txt)
    return [{"listing_id": listing_id, "agent_id": str(agent).strip()} for agent in agents]
