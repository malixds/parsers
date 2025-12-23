"""
Improved pydantic schema for commercial real estate listings.
Goals:
- Keep maximum flexibility (strings where raw data from the site should be preserved),
  but add structure where it’s clearly useful (agents, photos, spaces).
- Added clear field descriptions in English.
- Dates are typed as date (can be switched to datetime if needed).
- Identifiers/ZIP/MLS are strings to avoid losing leading zeros or formatting.
- Photos/features/types are lists instead of a single string.
- Multiple agents are stored as a list instead of agent_1/2/3/4.
- Added “raw” fields for unparsed values.
"""
from datetime import date
from typing import List, Optional, Dict, Any

from pydantic import BaseModel, Field, EmailStr


class AgentData(BaseModel):
    """Information about one agent/broker."""

    name: Optional[str] = Field(None, description="Agent's full name")
    title: Optional[str] = Field(None, description="Position, e.g., Senior Vice President")
    license: Optional[str] = Field(None, description="Agent's license number")
    phone_primary: Optional[str] = Field(None, description="Primary phone number")
    phone_alt: Optional[str] = Field(None, description="Alternative phone number")
    email: Optional[EmailStr] = Field(None, description="Agent's email address")
    address: Optional[str] = Field(None, description="Agent's postal address")
    social_media: Optional[str] = Field(None, description="Link to agent's social media profile")
    photo_url: Optional[str] = Field(None, description="Agent's photo link")

    office_name: Optional[str] = Field(None, description="Brokerage/office name")
    office_address: Optional[str] = Field(None, description="Office address")
    office_phone: Optional[str] = Field(None, description="Office phone number")
    office_email: Optional[EmailStr] = Field(None, description="Office email address")
    office_social_media: Optional[str] = Field(None, description="Link to office social media profile")


class DbDTO(BaseModel):
    # Basic identifiers/links
    source_name: str = Field(..., description="Source name")
    listing_id: str = Field(..., description="Listing ID as shown on the source site")
    listing_link: str = Field(..., description="Full URL to the listing page")

    # Listing type/status
    listing_type: Optional[str] = Field(None, description="For Sale / For Lease / Both")
    listing_status: Optional[str] = Field(None, description="Active / Inactive / Sold / Leased")

    # Address (raw + components if parsed)
    address: str = Field(..., description="Full address as displayed on site")
    coordinates: Optional[str] = Field(None)
    location_description: Optional[str] = Field(None, description="Free-text description of location")
    building_number: Optional[str] = Field(None, description="Building number, e.g., '1946'")
    street_name: Optional[str] = Field(None, description="Street name, e.g., 'Stadium Drive'")
    unit_number: Optional[str] = Field(None, description="Unit/Suite number, kept as string")
    city: Optional[str] = Field(None, description="City name")
    state: Optional[str] = Field(None, description="State abbreviation")
    zipcode: Optional[str] = Field(None, description="ZIP code as string to keep formatting")

    # Property info
    property_name: Optional[str] = Field(None, description="Name of the property")
    property_type: Optional[str] = Field(None, description="High-level type: Office / Retail / Land / ...")
    subtype: Optional[str] = Field(None, description="Example: Resale")
    use_types: Optional[List[str]] = Field(None, description="Intended uses, e.g., restaurant, dental office")
    subuse: Optional[str] = Field(None)
    building_class: Optional[str] = Field(None, description="Building class: Class A/B/C")
    number_buildings: Optional[str] = Field(None)
    positioning_and_potential: Optional[str] = Field(None, description="Strong operational upside...")
    location_highlights: Optional[List[str]] = Field(None, description="Location highlights")
    year_built: Optional[int] = Field(None, description="Year built")
    year_built_and_renovated: Optional[str] = Field(None, description="Year built/renovated")
    year_remodeled: Optional[int] = Field(None, description="Year remodeled")
    site_plans: Optional[str] = Field(None, description="Indicates that detailed, officially approved site plans for development exist for this property, e.g. In Place")
    land_sf: Optional[str] = Field(None, description="Total land area in square feet and location type, e.g. 36,998.1 SF Infill Location")
    listing_provided_by: Optional[str] = Field(None, description="Listing provided by field")
    parcel_id: Optional[str] = Field(None)
    legal_information: Optional[str] = Field(None)
    free_standing: Optional[str] = Field(None)
    foundation: Optional[str] = Field(None)
    water_source: Optional[str] = Field(None)

    # Pricing (raw strings)
    sale_price: Optional[str] = Field(None, description="Sale price as shown on site")
    lease_price: Optional[str] = Field(None,
                                       description="General lease price/rate string (e.g., '$22/SF/YR' or '$3,200/mo')")

    # Sizes (raw strings)
    size: Optional[str] = Field(None,
                                description="General size string from the site (e.g., '5600 sqft' or '300-3400 sqft')")
    size_minimum: Optional[str] = Field(None, description="Minimum available size (raw string)")
    size_total: Optional[str] = Field(None, description="Total size (raw string)")
    building_size: Optional[str] = Field(None, description="Total building size (raw string)")
    proposed_building_size: Optional[str] = Field(None, description="Proposed size of building, e.g. 752,802 SF (47 Floors)")
    unit_size: Optional[str] = Field(None)
    lot_size: Optional[str] = Field(None)
    typical_floor_size: Optional[str] = Field(None)
    acreage: Optional[str] = Field(None)
    future_land_use: Optional[str] = Field(None)
    total_available_space: Optional[str] = Field(None, description="Total available space (raw string)")
    available_space: Optional[str] = Field(None, description="available space (raw string)")
    available_space_list: Optional[list[dict[str, str]]] = Field(None)
    available_space_for_lease: Optional[str] = Field(None, description="available space for lease (raw string)")
    exterior_space: Optional[str] = Field(None)
    availabilities: Optional[list[dict[str, Any]]] = Field(None)
    max_contiguous: Optional[str] = Field(None, description="Max Contiguous size")
    min_divisible: Optional[str] = Field(None, description="Min Divisible size")

    # Units/spaces (no nested model)
    spaces_available: Optional[str] = Field(None, description="Raw list of available units, e.g., '#102, #202, #333'")
    offices_same_details: Optional[str] = Field(None,
                                                description="Raw block with per-unit details (unit, size, details, lease price/type) as appears on site")
    approved_units: Optional[str] = Field(None, description="How many units approved, e.g. 499 Units approved Under Live Local Act")
    stories: Optional[str] = Field(None)
    number_of_units: Optional[str] = Field(None)

    # Lease terms
    lease_type: Optional[str] = Field(None, description="General lease type (e.g., Direct, Sublease)")
    lease_rate_type: Optional[str] = Field(None, description="FSG / NNN / Modified Gross, etc.")
    lease_term: Optional[str] = Field(None)
    sublease: Optional[str] = Field(None, description="Type of sublease")

    # Identifiers
    mls_number: Optional[str] = Field(None, description="MLS number as string")

    # Texts
    property_highlights: Optional[str] = Field(None, description="Short highlights/bullet points")
    property_description: Optional[str] = Field(None, description="Full property description text")
    property_details: Optional[str] = Field(None, description="Property details value")
    comments: Optional[str] = Field(None)
    zoning: Optional[str] = Field(None)

    # Dates
    listing_date: Optional[date] = Field(None, description="Listing publication date")
    last_updated: Optional[date] = Field(None, description="Last updated date")
    days_on_market: Optional[str] = Field(None, description="Raw string for days on market (e.g., '23 days on market')")
    date_available: Optional[str] = Field(None)

    # Taxes/features
    taxes: Optional[str] = Field(None, description="Taxes as string (e.g., '$18,000')")
    features: Optional[List[str]] = Field(None, description="List of features (e.g., 'garage', 'top floor')")
    interior: Optional[Dict[str, Any]] = Field(None, description="jsonb of interior")
    exterior: Optional[Dict[str, Any]] = Field(None, description="jsonb of exterior")
    parking_features: Optional[Dict[str, Any]] = Field(None)
    residence_info: Optional[List[str]] = Field(None)
    room_information: Optional[Dict[str, Any]] = Field(None, description="jsonb of room info")
    taxes_property: Optional[str] = Field(None)
    listing_details: Optional[Dict[str, Any]] = Field(None)

    # Media/files
    photos: Optional[List[str]] = Field(None, description="List of photo URLs")
    brochure_pdf: Optional[str] = Field(None, description="Link to marketing brochure PDF")
    virtual_tour: Optional[str] = Field(None, description="Link to marketing virtual tour")

    # Agents (list)
    agents: list[dict[str, Any]] | List[AgentData] | dict[str, Any] | None = Field(None, description="List of agents")
    agency_phone: Optional[str] = Field(None)

    is_deleted: Optional[bool] = Field(None)

    def __hash__(self):
        return hash(self.listing_id)  # или другого уникального поля

    def __eq__(self, other):
        if isinstance(other, DbDTO):
            return self.listing_id == other.listing_id
        return False
