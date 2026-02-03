"""Platform for Målerportal sensor integration."""

import asyncio
from datetime import datetime, timedelta, timezone
import logging
import re
from typing import Any, Optional, Union
import aiohttp

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMetaData,
    StatisticMeanType,
)
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    async_import_statistics,
    get_last_statistics,
    statistics_during_period,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    UnitOfEnergy,
    UnitOfVolume,
    UnitOfTemperature,
    UnitOfTime,
    UnitOfFrequency,
    UnitOfPower,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.util import Throttle
from homeassistant.util import dt as dt_util
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.helpers.restore_state import RestoreEntity

from .const import DOMAIN, DEFAULT_POLLING_INTERVAL

# Import event firing function - will be available after __init__ loads
EVENT_METER_UPDATED = f"{DOMAIN}_meter_updated"

_LOGGER = logging.getLogger(__name__)


def get_polling_interval(config: ConfigEntry) -> timedelta:
    """Get polling interval from config options."""
    interval = config.options.get("polling_interval", DEFAULT_POLLING_INTERVAL)
    return timedelta(minutes=interval)


async def async_setup_entry(
    hass: HomeAssistant, config: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    """Set up the sensor platform."""

    config_data = hass.data[DOMAIN][config.entry_id]
    installations: list[dict] = config_data["installations"]
    api_key: str = config_data["api_key"]
    smarthome_base_url: str = config_data["smarthome_base_url"]
    polling_interval = get_polling_interval(config)
    
    sensors = []

    for installation in installations:
        try:
            # Fetch meter counters to determine what sensors to create
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                try:
                    response = await session.get(
                        f"{smarthome_base_url}/installations/{installation['installationId']}/readings/latest",
                        headers={"ApiKey": api_key},
                    )
                    
                    if response.ok:
                        readings_data = await response.json()
                        meter_counters = readings_data.get("meterCounters", [])
                        
                        # Create sensors based on available meter counters
                        installation_sensors = create_sensors_from_counters(
                            installation, api_key, smarthome_base_url, meter_counters, polling_interval
                        )
                        sensors.extend(installation_sensors)
                    else:
                        _LOGGER.warning("Failed to fetch meter data for installation %s (HTTP %s), creating basic sensor", 
                                      installation["installationId"], response.status)
                        # Fallback to basic sensor
                        sensors.append(MaalerportalBasicSensor(installation, api_key, smarthome_base_url, polling_interval))
                except asyncio.TimeoutError:
                    _LOGGER.warning("Timeout fetching meter data for installation %s, creating basic sensor", 
                                  installation["installationId"])
                    sensors.append(MaalerportalBasicSensor(installation, api_key, smarthome_base_url, polling_interval))
                    
        except Exception as err:
            _LOGGER.error("Error setting up sensors for installation %s: %s", 
                         installation["installationId"], err)
            # Fallback to basic sensor
            sensors.append(MaalerportalBasicSensor(installation, api_key, smarthome_base_url, polling_interval))

    async_add_entities(sensors)


def create_sensors_from_counters(
    installation: dict, api_key: str, smarthome_base_url: str, meter_counters: list[dict], polling_interval: timedelta
) -> list[SensorEntity]:
    """Create appropriate sensors based on available meter counters."""
    sensors = []
    
    # Consumable counter types:
    # - For readingType="counter": Only meter reading sensor (direct from meter)
    # - For readingType="consumption": Statistics sensor (Energy Dashboard) + Consumption sensor
    consumable_counter_types = [
        "ColdWater", "HotWater", "ElectricityFromGrid", "ElectricityToGrid", "Heat"
    ]
    
    # Heat meter counter types
    supply_temp_types = ["SupplyTemp", "FlowTemp", "T1", "SupplyTemperature"]
    return_temp_types = ["ReturnTemp", "T2", "ReturnTemperature"]
    temp_diff_types = ["TempDiff", "DeltaT", "TemperatureDifference"]
    heat_power_types = ["Power", "Effect", "HeatPower"]
    heat_volume_types = ["Volume", "V1", "HeatVolume"]
    
    # Find primary counter for main sensor
    primary_counter = None
    for counter in meter_counters:
        if counter.get("isPrimary", False):
            primary_counter = counter
            break
    
    if not primary_counter and meter_counters:
        primary_counter = meter_counters[0]
    
    # Create sensors for primary counter based on reading type
    if primary_counter:
        reading_type = primary_counter.get("readingType", "counter").lower()
        counter_type = primary_counter.get("counterType", "")
        
        if reading_type == "consumption":
            # For consumption-based meters (e.g., electricity from grid operator):
            # Create statistics sensor for Energy Dashboard + consumption sensor for 30-day usage
            if counter_type in consumable_counter_types:
                sensors.append(MaalerportalStatisticSensor(
                    installation, api_key, smarthome_base_url, primary_counter, polling_interval
                ))
                sensors.append(MaalerportalConsumptionSensor(
                    installation, api_key, smarthome_base_url, primary_counter, polling_interval
                ))
        else:
            # For counter-based meters (cumulative readings):
            # Create main sensor showing current meter reading
            sensors.append(MaalerportalMainSensor(
                installation, api_key, smarthome_base_url, primary_counter, polling_interval
            ))
            # Also add statistics sensor to load historical data (last 30 days)
            if counter_type in consumable_counter_types:
                sensors.append(MaalerportalStatisticSensor(
                    installation, api_key, smarthome_base_url, primary_counter, polling_interval
                ))
        
        # Add price sensor for primary counter if price is available
        if primary_counter.get("pricePerUnit") is not None:
            sensors.append(MaalerportalPriceSensor(
                installation, api_key, smarthome_base_url, primary_counter, polling_interval
            ))
    
    # Create additional sensors for other counter types
    for counter in meter_counters:
        counter_type = counter.get("counterType", "")
        is_primary = counter.get("isPrimary", False)
        
        if counter_type == "BatteryDaysRemaining":
            sensors.append(MaalerportalBatterySensor(
                installation, api_key, smarthome_base_url, counter, polling_interval
            ))
        elif counter_type in ["DailyMaxAmbientTemp", "DailyMinAmbientTemp"]:
            sensors.append(MaalerportalTemperatureSensor(
                installation, api_key, smarthome_base_url, counter, polling_interval
            ))
        elif counter_type in ["DailyMaxWaterTemp", "DailyMinWaterTemp"]:
            sensors.append(MaalerportalWaterTemperatureSensor(
                installation, api_key, smarthome_base_url, counter, polling_interval
            ))
        elif counter_type in ["DailyMaxFlow1", "DailyMinFlow1"]:
            sensors.append(MaalerportalFlowSensor(
                installation, api_key, smarthome_base_url, counter, polling_interval
            ))
        elif counter_type == "AcousticNoise":
            sensors.append(MaalerportalNoiseSensor(
                installation, api_key, smarthome_base_url, counter, polling_interval
            ))
        # Heat meter specific sensors
        elif counter_type in supply_temp_types:
            sensors.append(MaalerportalSupplyTempSensor(
                installation, api_key, smarthome_base_url, counter, polling_interval
            ))
        elif counter_type in return_temp_types:
            sensors.append(MaalerportalReturnTempSensor(
                installation, api_key, smarthome_base_url, counter, polling_interval
            ))
        elif counter_type in temp_diff_types:
            sensors.append(MaalerportalTempDiffSensor(
                installation, api_key, smarthome_base_url, counter, polling_interval
            ))
        elif counter_type in heat_power_types:
            sensors.append(MaalerportalHeatPowerSensor(
                installation, api_key, smarthome_base_url, counter, polling_interval
            ))
        elif counter_type in heat_volume_types:
            sensors.append(MaalerportalHeatVolumeSensor(
                installation, api_key, smarthome_base_url, counter, polling_interval
            ))
        elif not is_primary and counter_type in consumable_counter_types:
            # Secondary meters - sensors depend on reading type
            reading_type = counter.get("readingType", "counter").lower()
            
            if reading_type == "consumption":
                # For consumption-based: statistics sensor + consumption sensor
                sensors.append(MaalerportalStatisticSensor(
                    installation, api_key, smarthome_base_url, counter, polling_interval
                ))
                sensors.append(MaalerportalConsumptionSensor(
                    installation, api_key, smarthome_base_url, counter, polling_interval
                ))
            else:
                # For counter-based: meter reading sensor + statistics for historical data
                sensors.append(MaalerportalSecondarySensor(
                    installation, api_key, smarthome_base_url, counter, polling_interval
                ))
                sensors.append(MaalerportalStatisticSensor(
                    installation, api_key, smarthome_base_url, counter, polling_interval
                ))
    
    return sensors


class MaalerportalBaseSensor(SensorEntity):
    """Base class for all Målerportal sensors."""
    
    _attr_has_entity_name = True
    
    # Translation map for installation/meter types
    METER_TYPE_TRANSLATIONS = {
        "ColdWater": "Koldt vand",
        "HotWater": "Varmt vand",
        "Electricity": "El",
        "Heat": "Varme",
        "Gas": "Gas",
    }

    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str, 
        counter: dict = None,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the sensor."""
        self._installation = installation
        self._api_key = api_key
        self._smarthome_base_url = smarthome_base_url
        self._installation_id = installation["installationId"]
        self._installation_type = installation["installationType"]
        self._counter = counter
        self._polling_interval = polling_interval
        
        # Rate limiting
        self._rate_limit_delay = 2000  # 2 seconds between requests
        
        # Last contact tracking
        self._last_contact: Optional[datetime] = None
        self._last_reading_timestamp: Optional[str] = None
        
        # Installation availability tracking (for 404 handling)
        self._installation_available: bool = True
        self._last_availability_check: Optional[datetime] = None
        self._unavailable_since: Optional[datetime] = None
        self._availability_check_count: int = 0
        self._base_check_interval = timedelta(minutes=15)  # Base interval: 15 minutes
        self._max_check_interval = timedelta(hours=24)  # Max interval: 24 hours
        self._max_unavailable_days: int = 30  # Stop checking after 30 days
        
        # Create base device name
        self._base_device_name = f"{installation['address']} - {installation['meterSerial']}"
        if installation.get("nickname"):
            self._base_device_name += f" ({installation['nickname']})"

    def _get_translated_meter_type(self) -> str:
        """Get translated meter type for device model."""
        return self.METER_TYPE_TRANSLATIONS.get(self._installation_type, self._installation_type)

    @property
    def device_info(self) -> DeviceInfo:
        """Return device information."""
        return DeviceInfo(
            identifiers={(DOMAIN, self._installation_id)},
            name=self._base_device_name,
            manufacturer=self._installation.get("utilityName", "Unknown"),
            model=self._get_translated_meter_type(),
            serial_number=self._installation.get("meterSerial"),
        )

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes."""
        attrs: dict[str, Any] = {
            "installation_id": self._installation_id,
        }
        if self._last_contact:
            attrs["last_contact"] = self._last_contact.isoformat()
        if self._last_reading_timestamp:
            attrs["last_reading_timestamp"] = self._last_reading_timestamp
        if self._counter:
            attrs["counter_type"] = self._counter.get("counterType", "")
            attrs["meter_counter_id"] = self._counter.get("meterCounterId", "")
        return attrs

    async def async_update(self) -> None:
        """Fetch data from API."""
        # If installation is unavailable, only do periodic availability checks
        if not self._installation_available:
            await self._check_installation_availability()
            return
        
        try:
            _LOGGER.debug("Fetching meter readings for installation: %s", self._installation_id)
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                # Get latest readings from new API
                response = await session.get(
                    f"{self._smarthome_base_url}/installations/{self._installation_id}/readings/latest",
                    headers={"ApiKey": self._api_key},
                )

                if response.status == 429:
                    _LOGGER.warning("Rate limit exceeded, will retry later")
                    self._rate_limit_delay = min(self._rate_limit_delay * 2, 10000)
                    return
                
                # Handle 404/403 - installation no longer accessible
                if response.status in (404, 403):
                    _LOGGER.warning(
                        "Installation %s no longer accessible (HTTP %s), starting availability checks",
                        self._installation_id,
                        response.status
                    )
                    await self._handle_installation_unavailable()
                    return
                    
                if not response.ok:
                    _LOGGER.error("API request failed: HTTP %s", response.status)
                    return

                # Reset rate limit delay on success
                self._rate_limit_delay = 2000
                
                # Update last contact time
                self._last_contact = datetime.now()

                readings_data = await response.json()
                _LOGGER.debug("Readings data: %s", readings_data)

                if readings_data.get("meterCounters"):
                    await self._update_from_meter_counters(readings_data["meterCounters"])
                else:
                    _LOGGER.debug("No meter counters found in API response")

        except asyncio.TimeoutError:
            _LOGGER.warning("Timeout fetching meter readings for installation: %s", self._installation_id)
        except aiohttp.ClientError as err:
            _LOGGER.error("Connection error: %s", err)
        except Exception as err:
            _LOGGER.exception("Unexpected error fetching meter readings: %s", err)

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update sensor state from meter counter data - to be implemented by subclasses."""
        pass

    async def _handle_installation_unavailable(self) -> None:
        """Handle installation becoming unavailable (404/403 response).
        
        Marks the installation as unavailable and sets up periodic availability checks
        with exponential backoff.
        """
        if not self._installation_available:
            # Already marked as unavailable
            return
        
        self._installation_available = False
        self._unavailable_since = datetime.now(timezone.utc)
        self._availability_check_count = 0
        self._last_availability_check = datetime.now(timezone.utc)
        _LOGGER.warning(
            "Installation %s marked as unavailable. Will check periodically for restoration (max %d days).",
            self._installation_id,
            self._max_unavailable_days
        )

    def _get_current_check_interval(self) -> timedelta:
        """Calculate current check interval using exponential backoff.
        
        Starts at 15 minutes, doubles each check, max 24 hours.
        """
        # Exponential backoff: 15min, 30min, 1hr, 2hr, 4hr, 8hr, 16hr, 24hr (max)
        multiplier = 2 ** self._availability_check_count
        interval = self._base_check_interval * multiplier
        return min(interval, self._max_check_interval)

    async def _check_installation_availability(self) -> bool:
        """Check if the installation is available again.
        
        Checks the /addresses endpoint to see if the installation exists.
        Uses exponential backoff and stops after max_unavailable_days.
        
        Returns True if installation is available, False otherwise.
        """
        now = datetime.now(timezone.utc)
        
        # Check if we've exceeded max unavailable days - stop checking
        if self._unavailable_since:
            days_unavailable = (now - self._unavailable_since).days
            if days_unavailable >= self._max_unavailable_days:
                _LOGGER.warning(
                    "Installation %s has been unavailable for %d days (max: %d). "
                    "Stopping availability checks. Re-configure integration if access is restored.",
                    self._installation_id,
                    days_unavailable,
                    self._max_unavailable_days
                )
                return False
        
        # Calculate current check interval based on exponential backoff
        current_interval = self._get_current_check_interval()
        
        # Throttle availability checks based on current interval
        if self._last_availability_check:
            time_since_last_check = now - self._last_availability_check
            if time_since_last_check < current_interval:
                _LOGGER.debug(
                    "Skipping availability check (last check was %s ago, current interval is %s)",
                    time_since_last_check,
                    current_interval
                )
                return False
        
        # Increment check count for exponential backoff
        self._availability_check_count += 1
        self._last_availability_check = now
        
        try:
            _LOGGER.debug(
                "Checking if installation %s is available again (check #%d, next interval: %s)...",
                self._installation_id,
                self._availability_check_count,
                self._get_current_check_interval()
            )
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                response = await session.get(
                    f"{self._smarthome_base_url}/addresses",
                    headers={"ApiKey": self._api_key},
                )
                
                if response.status == 429:
                    _LOGGER.warning("Rate limit exceeded during availability check")
                    return False
                
                if not response.ok:
                    _LOGGER.debug("Availability check failed: HTTP %s", response.status)
                    return False
                
                addresses = await response.json()
                
                # Check if our installation exists in the list
                installation_found = False
                for address in addresses:
                    for installation in address.get("installations", []):
                        if installation.get("installationId") == self._installation_id:
                            installation_found = True
                            break
                    if installation_found:
                        break
                
                if installation_found:
                    _LOGGER.info(
                        "Installation %s found again! Resuming normal operation.",
                        self._installation_id
                    )
                    # Reset all availability tracking state
                    self._installation_available = True
                    self._unavailable_since = None
                    self._availability_check_count = 0
                    return True
                else:
                    _LOGGER.debug(
                        "Installation %s still not found (check #%d), next check in %s",
                        self._installation_id,
                        self._availability_check_count,
                        self._get_current_check_interval()
                    )
                    return False
                    
        except asyncio.TimeoutError:
            _LOGGER.warning("Timeout during availability check for installation %s", self._installation_id)
        except aiohttp.ClientError as err:
            _LOGGER.error("Connection error during availability check: %s", err)
        except Exception as err:
            _LOGGER.exception("Unexpected error during availability check: %s", err)
        
        return False

    def _parse_counter_value(self, counter: dict) -> Optional[float]:
        """Parse and validate counter value."""
        latest_value = counter.get("latestValue")
        if latest_value is None:
            return None
            
        try:
            if isinstance(latest_value, (int, float)):
                numeric_value = float(latest_value)
            elif isinstance(latest_value, str):
                # Clean the string value - remove any non-numeric characters except decimal point
                cleaned_value = latest_value.strip()
                cleaned_value = re.sub(r'[^\d.-]', '', cleaned_value)
                numeric_value = float(cleaned_value)
            else:
                _LOGGER.error("Unexpected value type: %s", type(latest_value))
                return None
                
            # Validate the number
            if not (isinstance(numeric_value, (int, float)) and not (numeric_value != numeric_value)):  # Check for NaN
                _LOGGER.error("Invalid numeric value: %s", latest_value)
                return None
            
            # Update last reading timestamp
            if counter.get("latestTimestamp"):
                self._last_reading_timestamp = counter.get("latestTimestamp")
                
            return numeric_value
            
        except (ValueError, TypeError) as err:
            _LOGGER.error("Error parsing meter value '%s': %s", latest_value, err)
            return None


class MaalerportalMainSensor(MaalerportalBaseSensor):
    """Main sensor for primary meter counter."""

    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str, 
        counter: dict,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the main sensor."""
        super().__init__(installation, api_key, smarthome_base_url, counter, polling_interval)
        
        self._attr_unique_id = f"{self._installation_id}_main"
        
        # Set attributes based on counter type
        counter_type = counter.get("counterType", "").lower()
        if counter_type in ["coldwater", "hotwater"]:
            self._attr_translation_key = "meter_reading_water"
            self._attr_device_class = SensorDeviceClass.WATER
            self._attr_native_unit_of_measurement = UnitOfVolume.CUBIC_METERS
            self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        elif counter_type in ["electricityfromgrid", "electricitytogrid"]:
            self._attr_translation_key = "meter_reading_electricity"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
            self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        elif counter_type == "heat":
            self._attr_translation_key = "meter_reading_heat"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
            self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        else:
            self._attr_translation_key = "meter_reading"
            self._attr_device_class = None
            self._attr_native_unit_of_measurement = counter.get("unit")
            self._attr_state_class = SensorStateClass.MEASUREMENT

    @Throttle(timedelta(minutes=30))
    async def async_update(self) -> None:
        """Fetch data from API with throttling."""
        await super().async_update()

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update main sensor from primary counter."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    # Ensure positive value for meters
                    if value < 0:
                        _LOGGER.warning("Negative meter value received, taking absolute value: %s", value)
                        value = abs(value)
                    
                    # Check if value changed before firing event
                    old_value = self._attr_native_value
                    self._attr_native_value = value
                    _LOGGER.debug("Updated main sensor value: %s %s", value, counter.get("unit", ""))
                    
                    # Fire event if value changed and hass is available
                    if self.hass and (old_value is None or old_value != value):
                        self.hass.bus.fire(EVENT_METER_UPDATED, {
                            "installation_id": self._installation_id,
                            "meter_value": value,
                            "unit": counter.get("unit", ""),
                            "counter_type": counter.get("counterType", ""),
                            "timestamp": counter.get("latestTimestamp"),
                        })
                        _LOGGER.debug("Fired %s event for installation %s", EVENT_METER_UPDATED, self._installation_id)
                break


class MaalerportalBasicSensor(MaalerportalBaseSensor):
    """Basic fallback sensor when meter data is not available."""

    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the basic sensor."""
        super().__init__(installation, api_key, smarthome_base_url, polling_interval=polling_interval)
        
        self._attr_unique_id = f"{self._installation_id}_basic"
        
        # Set default attributes based on installation type
        installation_type = self._installation_type.lower()
        if installation_type in ["coldwater", "hotwater"]:
            self._attr_translation_key = "meter_reading_water"
            self._attr_device_class = SensorDeviceClass.WATER
            self._attr_native_unit_of_measurement = UnitOfVolume.CUBIC_METERS
            self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        elif installation_type == "electricity":
            self._attr_translation_key = "meter_reading_electricity"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
            self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        elif installation_type == "heat":
            self._attr_translation_key = "meter_reading_heat"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
            self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        else:
            self._attr_translation_key = "meter_reading"
            self._attr_device_class = None
            self._attr_native_unit_of_measurement = None
            self._attr_state_class = SensorStateClass.MEASUREMENT

    @Throttle(timedelta(minutes=30))
    async def async_update(self) -> None:
        """Fetch data from API with throttling."""
        await super().async_update()

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update basic sensor from first available counter."""
        if meter_counters:
            # Find primary counter or use first one
            primary_counter = None
            for counter in meter_counters:
                if counter.get("isPrimary", False):
                    primary_counter = counter
                    break
            
            if not primary_counter:
                primary_counter = meter_counters[0]
            
            value = self._parse_counter_value(primary_counter)
            if value is not None:
                if value < 0:
                    value = abs(value)
                
                self._attr_native_value = value
                _LOGGER.debug("Updated basic sensor value: %s %s", value, primary_counter.get("unit", ""))


# Specialized sensor classes for different counter types

class MaalerportalBatterySensor(MaalerportalBaseSensor):
    """Battery days remaining sensor."""

    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str, 
        counter: dict,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the battery sensor."""
        super().__init__(installation, api_key, smarthome_base_url, counter, polling_interval)
        
        self._attr_translation_key = "battery_days"
        self._attr_unique_id = f"{self._installation_id}_battery_days"
        self._attr_device_class = SensorDeviceClass.DURATION
        self._attr_native_unit_of_measurement = UnitOfTime.DAYS
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:battery"

    @Throttle(timedelta(minutes=30))
    async def async_update(self) -> None:
        """Fetch data from API with throttling."""
        await super().async_update()

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update battery sensor."""
        for counter in meter_counters:
            if counter.get("counterType") == "BatteryDaysRemaining":
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = int(value)
                    _LOGGER.debug("Updated battery days: %s", value)
                break


class MaalerportalTemperatureSensor(MaalerportalBaseSensor):
    """Ambient temperature sensor."""

    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str, 
        counter: dict,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the temperature sensor."""
        super().__init__(installation, api_key, smarthome_base_url, counter, polling_interval)
        
        counter_type = counter.get("counterType", "")
        if "Max" in counter_type:
            self._attr_translation_key = "max_ambient_temperature"
            self._attr_unique_id = f"{self._installation_id}_temp_ambient_max"
        else:
            self._attr_translation_key = "min_ambient_temperature"
            self._attr_unique_id = f"{self._installation_id}_temp_ambient_min"
            
        self._attr_device_class = SensorDeviceClass.TEMPERATURE
        self._attr_native_unit_of_measurement = UnitOfTemperature.CELSIUS
        self._attr_state_class = SensorStateClass.MEASUREMENT

    @Throttle(timedelta(minutes=30))
    async def async_update(self) -> None:
        """Fetch data from API with throttling."""
        await super().async_update()

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update temperature sensor."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = round(value, 1)
                    _LOGGER.debug("Updated ambient temperature: %s°C", value)
                break


class MaalerportalWaterTemperatureSensor(MaalerportalBaseSensor):
    """Water temperature sensor."""

    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str, 
        counter: dict,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the water temperature sensor."""
        super().__init__(installation, api_key, smarthome_base_url, counter, polling_interval)
        
        counter_type = counter.get("counterType", "")
        if "Max" in counter_type:
            self._attr_translation_key = "max_water_temperature"
            self._attr_unique_id = f"{self._installation_id}_temp_water_max"
        else:
            self._attr_translation_key = "min_water_temperature"
            self._attr_unique_id = f"{self._installation_id}_temp_water_min"
            
        self._attr_device_class = SensorDeviceClass.TEMPERATURE
        self._attr_native_unit_of_measurement = UnitOfTemperature.CELSIUS
        self._attr_state_class = SensorStateClass.MEASUREMENT

    @Throttle(timedelta(minutes=30))
    async def async_update(self) -> None:
        """Fetch data from API with throttling."""
        await super().async_update()

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update water temperature sensor."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = round(value, 1)
                    _LOGGER.debug("Updated water temperature: %s°C", value)
                break


class MaalerportalFlowSensor(MaalerportalBaseSensor):
    """Flow sensor."""

    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str, 
        counter: dict,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the flow sensor."""
        super().__init__(installation, api_key, smarthome_base_url, counter, polling_interval)
        
        counter_type = counter.get("counterType", "")
        if "Max" in counter_type:
            self._attr_translation_key = "max_flow"
            self._attr_unique_id = f"{self._installation_id}_flow_max"
        else:
            self._attr_translation_key = "min_flow"
            self._attr_unique_id = f"{self._installation_id}_flow_min"
            
        self._attr_native_unit_of_measurement = "L/h"
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:water-pump"

    @Throttle(timedelta(minutes=30))
    async def async_update(self) -> None:
        """Fetch data from API with throttling."""
        await super().async_update()

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update flow sensor."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = round(value, 3)
                    _LOGGER.debug("Updated flow: %s L/h", value)
                break


class MaalerportalNoiseSensor(MaalerportalBaseSensor):
    """Acoustic noise sensor."""

    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str, 
        counter: dict,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the noise sensor."""
        super().__init__(installation, api_key, smarthome_base_url, counter, polling_interval)
        
        self._attr_translation_key = "acoustic_noise"
        self._attr_unique_id = f"{self._installation_id}_acoustic_noise"
        self._attr_native_unit_of_measurement = UnitOfFrequency.HERTZ
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:volume-high"

    @Throttle(timedelta(minutes=30))
    async def async_update(self) -> None:
        """Fetch data from API with throttling."""
        await super().async_update()

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update noise sensor."""
        for counter in meter_counters:
            if counter.get("counterType") == "AcousticNoise":
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = int(value)
                    _LOGGER.debug("Updated acoustic noise: %s Hz", value)
                break


class MaalerportalSecondarySensor(MaalerportalBaseSensor):
    """Secondary meter sensor."""

    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str, 
        counter: dict,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the secondary sensor."""
        super().__init__(installation, api_key, smarthome_base_url, counter, polling_interval)
        
        counter_type = counter.get("counterType", "")
        
        # Set translation key based on counter type
        if counter_type == "ColdWater":
            self._attr_translation_key = "cold_water"
            self._attr_device_class = SensorDeviceClass.WATER
            self._attr_native_unit_of_measurement = UnitOfVolume.CUBIC_METERS
        elif counter_type == "HotWater":
            self._attr_translation_key = "hot_water"
            self._attr_device_class = SensorDeviceClass.WATER
            self._attr_native_unit_of_measurement = UnitOfVolume.CUBIC_METERS
        elif counter_type == "ElectricityFromGrid":
            self._attr_translation_key = "electricity_import"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
        elif counter_type == "ElectricityToGrid":
            self._attr_translation_key = "electricity_export"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
        elif counter_type == "Heat":
            self._attr_translation_key = "heat"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
        else:
            self._attr_translation_key = "meter_reading"
            self._attr_device_class = None
            self._attr_native_unit_of_measurement = counter.get("unit")
        
        self._attr_unique_id = f"{self._installation_id}_{counter_type.lower()}_secondary"
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING

    @Throttle(timedelta(minutes=30))
    async def async_update(self) -> None:
        """Fetch data from API with throttling."""
        await super().async_update()

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update secondary sensor."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    if value < 0:
                        value = abs(value)
                    
                    # Check if value changed before firing event
                    old_value = self._attr_native_value
                    self._attr_native_value = value
                    _LOGGER.debug("Updated secondary sensor %s: %s %s", 
                                self._counter.get("counterType"), value, counter.get("unit", ""))
                    
                    # Fire event if value changed and hass is available
                    if self.hass and (old_value is None or old_value != value):
                        self.hass.bus.fire(EVENT_METER_UPDATED, {
                            "installation_id": self._installation_id,
                            "meter_value": value,
                            "unit": counter.get("unit", ""),
                            "counter_type": counter.get("counterType", ""),
                            "timestamp": counter.get("latestTimestamp"),
                        })
                break


class MaalerportalPriceSensor(MaalerportalBaseSensor):
    """Price per unit sensor for primary counters."""

    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str, 
        counter: dict,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the price sensor."""
        super().__init__(installation, api_key, smarthome_base_url, counter, polling_interval)
        
        unit = counter.get("unit", "unit")
        self._attr_translation_key = "price_per_unit"
        self._attr_unique_id = f"{self._installation_id}_price_per_unit"
        self._attr_native_unit_of_measurement = f"kr/{unit}"
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:currency-usd"

    @Throttle(timedelta(minutes=30))
    async def async_update(self) -> None:
        """Fetch data from API with throttling."""
        await super().async_update()

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update price sensor."""
        for counter in meter_counters:
            if (counter.get("meterCounterId") == self._counter.get("meterCounterId") and 
                counter.get("isPrimary", False)):
                price_per_unit = counter.get("pricePerUnit")
                if price_per_unit is not None:
                    # Convert from øre to kroner (divide by 100)
                    price_in_kroner = price_per_unit / 100
                    self._attr_native_value = round(price_in_kroner, 4)
                    _LOGGER.debug("Updated price per unit: %s kr/%s", 
                                price_in_kroner, counter.get("unit", ""))
                break


class MaalerportalConsumptionSensor(MaalerportalBaseSensor, RestoreEntity):
    """Consumption sensor that shows virtual cumulative meter reading.
    
    For consumption-type meters (readingType=consumption), this sensor tracks
    a virtual cumulative meter reading by summing all consumption values.
    This is useful for display purposes while the StatisticSensor handles
    Energy Dashboard integration.
    """

    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str, 
        counter: dict,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the consumption sensor."""
        super().__init__(installation, api_key, smarthome_base_url, counter, polling_interval)
        
        counter_type = counter.get("counterType", "")
        is_primary = counter.get("isPrimary", False)
        
        # Get installation type to distinguish hot vs cold water
        # (counter_type may always be "ColdWater" for water meters)
        installation_type = installation.get("installationType", "").lower()
        
        # Set translation key based on counter type - these show virtual meter reading
        if counter_type == "ColdWater" or counter_type == "HotWater":
            # Use installation type to determine if it's hot or cold water
            if installation_type == "hotwater":
                self._attr_translation_key = "virtual_hot_water_meter"
            else:
                self._attr_translation_key = "virtual_cold_water_meter"
            self._attr_device_class = SensorDeviceClass.WATER
            self._attr_native_unit_of_measurement = UnitOfVolume.CUBIC_METERS
        elif counter_type == "ElectricityFromGrid":
            self._attr_translation_key = "virtual_electricity_meter"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
        elif counter_type == "ElectricityToGrid":
            self._attr_translation_key = "virtual_electricity_export_meter"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
        elif counter_type == "Heat":
            self._attr_translation_key = "virtual_heat_meter"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
        else:
            self._attr_translation_key = "virtual_meter"
            self._attr_device_class = None
            self._attr_native_unit_of_measurement = counter.get("unit")
        
        # Create unique ID based on counter type and primary/secondary status
        suffix = "primary" if is_primary else "secondary"
        self._attr_unique_id = f"{self._installation_id}_{counter_type.lower()}_consumption_{suffix}"
        
        # Use TOTAL_INCREASING for cumulative virtual meter reading
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        self._attr_icon = "mdi:counter"
        
        # Virtual cumulative meter reading
        self._cumulative_sum: float = 0.0
        self._last_processed_timestamp: Optional[datetime] = None
        self._initialized: bool = False

    @property
    def native_value(self) -> Optional[float]:
        """Return the virtual cumulative meter reading."""
        if self._cumulative_sum > 0:
            return round(self._cumulative_sum, 3)
        return None

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes for persistence."""
        attrs = super().extra_state_attributes
        if self._cumulative_sum > 0:
            attrs["cumulative_sum"] = self._cumulative_sum
        if self._last_processed_timestamp:
            attrs["last_processed_timestamp"] = self._last_processed_timestamp.isoformat()
        attrs["virtual_meter"] = True
        return attrs

    async def async_added_to_hass(self) -> None:
        """Restore previous state when entity is added to hass."""
        await super().async_added_to_hass()
        
        # Restore previous state
        last_state = await self.async_get_last_state()
        if last_state is not None:
            # Restore cumulative sum from state
            if last_state.state not in (None, "unknown", "unavailable"):
                try:
                    self._cumulative_sum = float(last_state.state)
                    _LOGGER.debug(
                        "Restored virtual meter reading for %s: %s",
                        self._attr_unique_id,
                        self._cumulative_sum
                    )
                except (ValueError, TypeError):
                    pass
            
            # Restore last processed timestamp from attributes
            if last_state.attributes:
                timestamp_str = last_state.attributes.get("last_processed_timestamp")
                if timestamp_str:
                    try:
                        self._last_processed_timestamp = datetime.fromisoformat(timestamp_str)
                    except (ValueError, TypeError):
                        pass
        
        self._initialized = True
        # Fetch initial data
        await self._fetch_and_accumulate()

    @Throttle(timedelta(hours=1))
    async def async_update(self) -> None:
        """Fetch new consumption data and update cumulative sum."""
        # If installation is unavailable, only do periodic availability checks
        if not self._installation_available:
            await self._check_installation_availability()
            return
        
        if self._initialized:
            await self._fetch_and_accumulate()

    async def _fetch_and_accumulate(self) -> None:
        """Fetch historical consumption data and accumulate into virtual meter reading."""
        try:
            counter_id = self._counter.get("meterCounterId")
            if not counter_id:
                _LOGGER.warning("No meterCounterId available for consumption sensor")
                return
            
            now = datetime.now(timezone.utc)
            self._last_contact = now
            
            # Calculate date range - fetch data since last processed, or last 30 days if first time
            if self._last_processed_timestamp:
                start_date = self._last_processed_timestamp
            else:
                start_date = now - timedelta(days=30)
            
            end_date = now
            
            # Format as ISO datetime strings for the API
            start_date_iso = start_date.strftime("%Y-%m-%dT00:00:00Z")
            end_date_iso = end_date.strftime("%Y-%m-%dT23:59:59Z")
            
            _LOGGER.debug("Fetching consumption for counter %s from %s to %s", 
                          counter_id, start_date_iso, end_date_iso)
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                # Use POST endpoint with JSON body
                response = await session.post(
                    f"{self._smarthome_base_url}/installations/{self._installation_id}/readings/historical",
                    json={"from": start_date_iso, "to": end_date_iso},
                    headers={"ApiKey": self._api_key, "Content-Type": "application/json"},
                )
                
                if response.status == 429:
                    _LOGGER.warning("Rate limit exceeded for historical data, will retry later")
                    return
                
                # Handle 404/403 - installation no longer accessible
                if response.status in (404, 403):
                    _LOGGER.warning(
                        "Installation %s no longer accessible (HTTP %s)",
                        self._installation_id,
                        response.status
                    )
                    await self._handle_installation_unavailable()
                    return
                
                if not response.ok:
                    _LOGGER.error("Historical data request failed: HTTP %s", response.status)
                    return
                
                historical_data = await response.json()
                
                # Process readings for this specific counter
                readings = historical_data.get("readings", [])
                new_consumption = 0.0
                valid_readings = 0
                latest_timestamp: Optional[datetime] = None
                
                for reading in readings:
                    # Filter by counter ID
                    if reading.get("meterCounterId") != counter_id:
                        continue
                    
                    # Parse timestamp
                    timestamp_str = reading.get("timestamp")
                    if not timestamp_str:
                        continue
                    
                    try:
                        # Handle both Z suffix and explicit timezone offsets
                        if timestamp_str.endswith("Z"):
                            reading_timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                        else:
                            reading_timestamp = datetime.fromisoformat(timestamp_str)
                        # Convert to UTC for consistency
                        reading_timestamp = reading_timestamp.astimezone(timezone.utc)
                    except (ValueError, TypeError):
                        continue
                    
                    # Skip if already processed
                    if self._last_processed_timestamp and reading_timestamp <= self._last_processed_timestamp:
                        continue
                    
                    # Use the value field (consumption is None for grid operator data)
                    consumption = reading.get("value")
                    if consumption is not None:
                        try:
                            if isinstance(consumption, (int, float)):
                                numeric_value = float(consumption)
                            elif isinstance(consumption, str):
                                cleaned_value = consumption.strip()
                                cleaned_value = re.sub(r'[^\d.-]', '', cleaned_value)
                                numeric_value = float(cleaned_value)
                            else:
                                continue
                            
                            if numeric_value >= 0:  # Only add positive values
                                new_consumption += numeric_value
                                valid_readings += 1
                                
                                # Track latest timestamp
                                if latest_timestamp is None or reading_timestamp > latest_timestamp:
                                    latest_timestamp = reading_timestamp
                        except (ValueError, TypeError):
                            continue
                
                if valid_readings > 0:
                    self._cumulative_sum += new_consumption
                    if latest_timestamp:
                        self._last_processed_timestamp = latest_timestamp
                    
                    _LOGGER.debug(
                        "Updated virtual meter for %s: added %s, total now %s %s (from %d new readings)",
                        self._counter.get("counterType"),
                        new_consumption,
                        self._cumulative_sum,
                        self._attr_native_unit_of_measurement,
                        valid_readings
                    )
                else:
                    _LOGGER.debug("No valid readings found for consumption calculation")
                    
        except asyncio.TimeoutError:
            _LOGGER.warning("Timeout fetching historical data for consumption sensor")
        except aiohttp.ClientError as err:
            _LOGGER.error("Connection error fetching historical data: %s", err)
        except Exception as err:
            _LOGGER.exception("Unexpected error fetching historical data: %s", err)

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Not used for consumption sensor - we fetch historical data directly."""
        pass


# Heat meter specific sensors

class MaalerportalSupplyTempSensor(MaalerportalBaseSensor):
    """Supply/flow temperature sensor for heat meters."""

    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str, 
        counter: dict,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the supply temperature sensor."""
        super().__init__(installation, api_key, smarthome_base_url, counter, polling_interval)
        
        self._attr_translation_key = "supply_temperature"
        self._attr_unique_id = f"{self._installation_id}_temp_supply"
        self._attr_device_class = SensorDeviceClass.TEMPERATURE
        self._attr_native_unit_of_measurement = UnitOfTemperature.CELSIUS
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:thermometer-chevron-up"

    @Throttle(timedelta(minutes=30))
    async def async_update(self) -> None:
        """Fetch data from API with throttling."""
        await super().async_update()

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update supply temperature sensor."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = round(value, 1)
                    _LOGGER.debug("Updated supply temperature: %s°C", value)
                break


class MaalerportalReturnTempSensor(MaalerportalBaseSensor):
    """Return temperature sensor for heat meters."""

    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str, 
        counter: dict,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the return temperature sensor."""
        super().__init__(installation, api_key, smarthome_base_url, counter, polling_interval)
        
        self._attr_translation_key = "return_temperature"
        self._attr_unique_id = f"{self._installation_id}_temp_return"
        self._attr_device_class = SensorDeviceClass.TEMPERATURE
        self._attr_native_unit_of_measurement = UnitOfTemperature.CELSIUS
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:thermometer-chevron-down"

    @Throttle(timedelta(minutes=30))
    async def async_update(self) -> None:
        """Fetch data from API with throttling."""
        await super().async_update()

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update return temperature sensor."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = round(value, 1)
                    _LOGGER.debug("Updated return temperature: %s°C", value)
                break


class MaalerportalTempDiffSensor(MaalerportalBaseSensor):
    """Temperature difference sensor for heat meters."""

    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str, 
        counter: dict,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the temperature difference sensor."""
        super().__init__(installation, api_key, smarthome_base_url, counter, polling_interval)
        
        self._attr_translation_key = "temperature_difference"
        self._attr_unique_id = f"{self._installation_id}_temp_diff"
        self._attr_device_class = SensorDeviceClass.TEMPERATURE
        self._attr_native_unit_of_measurement = UnitOfTemperature.CELSIUS
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:thermometer-lines"

    @Throttle(timedelta(minutes=30))
    async def async_update(self) -> None:
        """Fetch data from API with throttling."""
        await super().async_update()

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update temperature difference sensor."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = round(value, 1)
                    _LOGGER.debug("Updated temperature difference: %s°C", value)
                break


class MaalerportalHeatPowerSensor(MaalerportalBaseSensor):
    """Heat power/effect sensor for heat meters."""

    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str, 
        counter: dict,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the heat power sensor."""
        super().__init__(installation, api_key, smarthome_base_url, counter, polling_interval)
        
        self._attr_translation_key = "heat_power"
        self._attr_unique_id = f"{self._installation_id}_heat_power"
        self._attr_device_class = SensorDeviceClass.POWER
        self._attr_native_unit_of_measurement = UnitOfPower.KILO_WATT
        self._attr_state_class = SensorStateClass.MEASUREMENT
        self._attr_icon = "mdi:fire"

    @Throttle(timedelta(minutes=30))
    async def async_update(self) -> None:
        """Fetch data from API with throttling."""
        await super().async_update()

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update heat power sensor."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = round(value, 2)
                    _LOGGER.debug("Updated heat power: %s kW", value)
                break


class MaalerportalHeatVolumeSensor(MaalerportalBaseSensor):
    """Heat volume sensor for heat meters."""

    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str, 
        counter: dict,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the heat volume sensor."""
        super().__init__(installation, api_key, smarthome_base_url, counter, polling_interval)
        
        self._attr_translation_key = "heat_volume"
        self._attr_unique_id = f"{self._installation_id}_heat_volume"
        self._attr_device_class = SensorDeviceClass.WATER
        self._attr_native_unit_of_measurement = UnitOfVolume.CUBIC_METERS
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        self._attr_icon = "mdi:water-thermometer"

    @Throttle(timedelta(minutes=30))
    async def async_update(self) -> None:
        """Fetch data from API with throttling."""
        await super().async_update()

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Update heat volume sensor."""
        for counter in meter_counters:
            if counter.get("meterCounterId") == self._counter.get("meterCounterId"):
                value = self._parse_counter_value(counter)
                if value is not None:
                    self._attr_native_value = round(value, 3)
                    _LOGGER.debug("Updated heat volume: %s m³", value)
                break


class MaalerportalStatisticSensor(MaalerportalBaseSensor, RestoreEntity):
    """Statistics sensor for electricity meters - inserts historical data into HA's long-term statistics.
    
    This sensor is designed for meters where data is delayed (1-3 days from grid operator).
    It uses async_add_external_statistics to insert historical readings into Home Assistant's
    statistics database, making it compatible with the Energy Dashboard.
    
    For consumption-type meters (readingType="consumption"), this sensor also maintains a
    virtual cumulative meter reading by summing all historical consumption values. This
    virtual meter is restored across restarts using RestoreEntity.
    """

    def __init__(
        self, 
        installation: dict, 
        api_key: str, 
        smarthome_base_url: str, 
        counter: dict,
        polling_interval: timedelta = timedelta(minutes=30)
    ) -> None:
        """Initialize the statistics sensor."""
        super().__init__(installation, api_key, smarthome_base_url, counter, polling_interval)
        
        counter_type = counter.get("counterType", "")
        is_primary = counter.get("isPrimary", False)
        suffix = "primary" if is_primary else "secondary"
        
        # Get installation type to distinguish hot vs cold water
        # (counter_type may always be "ColdWater" for water meters)
        installation_type = installation.get("installationType", "").lower()
        
        # Set attributes based on counter type
        if counter_type == "ColdWater" or counter_type == "HotWater":
            # Use installation type to determine if it's hot or cold water
            if installation_type == "hotwater":
                self._attr_translation_key = "hot_water_statistic"
                self._attr_icon = "mdi:water-thermometer"
            else:
                self._attr_translation_key = "cold_water_statistic"
                self._attr_icon = "mdi:water"
            self._attr_device_class = SensorDeviceClass.WATER
            self._attr_native_unit_of_measurement = UnitOfVolume.CUBIC_METERS
            self._unit_class = "volume"
        elif counter_type == "ElectricityFromGrid":
            self._attr_translation_key = "electricity_statistic" if is_primary else "electricity_import_statistic"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
            self._attr_icon = "mdi:transmission-tower-import"
            self._unit_class = "energy"
        elif counter_type == "ElectricityToGrid":
            self._attr_translation_key = "electricity_export_statistic"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
            self._attr_icon = "mdi:transmission-tower-export"
            self._unit_class = "energy"
        elif counter_type == "Heat":
            self._attr_translation_key = "heat_statistic"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
            self._attr_icon = "mdi:fire"
            self._unit_class = "energy"
        else:
            self._attr_translation_key = "energy_statistic"
            self._attr_device_class = SensorDeviceClass.ENERGY
            self._attr_native_unit_of_measurement = UnitOfEnergy.KILO_WATT_HOUR
            self._attr_icon = "mdi:chart-line-variant"
            self._unit_class = "energy"
        
        self._attr_unique_id = f"{self._installation_id}_{counter_type.lower()}_statistic_{suffix}"
        self._attr_state_class = SensorStateClass.TOTAL_INCREASING
        
        # Reading type determines how we process data
        # "counter" = cumulative meter reading (use value directly)
        # "consumption" = per-period consumption (need to accumulate)
        self._reading_type = counter.get("readingType", "counter").lower()
        
        # Statistics metadata - will be set to entity_id after entity is added to HA
        self._statistic_id: Optional[str] = None  # Will be set in async_added_to_hass
        self._stat_unit = self._attr_native_unit_of_measurement
        self._last_stats_update: Optional[datetime] = None
        
        # Track last inserted timestamp and cumulative sum for consumption types
        self._last_inserted_timestamp: Optional[datetime] = None
        self._cumulative_sum: float = 0.0

    @property
    def native_value(self) -> Optional[float]:
        """Return None - this sensor is only for importing statistics to Energy Dashboard.
        
        The virtual cumulative sum is tracked internally for statistics import,
        but the sensor itself shows 'unknown' to avoid confusion.
        """
        return None

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        """Return extra state attributes for persistence."""
        attrs = {
            "reading_type": self._reading_type,
        }
        if self._statistic_id:
            attrs["statistic_id"] = self._statistic_id
        if self._cumulative_sum > 0:
            attrs["cumulative_sum"] = self._cumulative_sum
        if self._last_inserted_timestamp:
            attrs["last_inserted_timestamp"] = self._last_inserted_timestamp.isoformat()
        return attrs

    async def async_added_to_hass(self) -> None:
        """Run when entity is added to hass - restore state and fetch data."""
        await super().async_added_to_hass()
        
        # Set statistic_id to the sensor's entity_id (e.g., "sensor.xxx")
        # This allows async_import_statistics to insert data into this sensor's statistics
        self._statistic_id = self.entity_id
        _LOGGER.debug("Set statistic_id to entity_id: %s", self._statistic_id)
        
        # Restore previous state for consumption-type meters
        if self._reading_type == "consumption":
            last_state = await self.async_get_last_state()
            if last_state is not None:
                # Restore cumulative sum from state
                if last_state.state not in (None, "unknown", "unavailable"):
                    try:
                        self._cumulative_sum = float(last_state.state)
                        _LOGGER.debug(
                            "Restored cumulative sum for %s: %s",
                            self._attr_unique_id,
                            self._cumulative_sum
                        )
                    except (ValueError, TypeError):
                        pass
                
                # Restore last inserted timestamp from attributes
                if last_state.attributes:
                    timestamp_str = last_state.attributes.get("last_inserted_timestamp")
                    if timestamp_str:
                        try:
                            self._last_inserted_timestamp = datetime.fromisoformat(timestamp_str)
                            _LOGGER.debug(
                                "Restored last_inserted_timestamp for %s: %s",
                                self._attr_unique_id,
                                self._last_inserted_timestamp
                            )
                        except (ValueError, TypeError):
                            pass
        
        # Schedule initial statistics update
        self.hass.async_create_task(self._async_update_statistics())

    @Throttle(timedelta(minutes=30))
    async def async_update(self) -> None:
        """Update statistics from historical API data."""
        # If installation is unavailable, only do periodic availability checks
        if not self._installation_available:
            await self._check_installation_availability()
            return
        
        await self._async_update_statistics()

    async def _async_update_statistics(self) -> None:
        """Fetch historical data and insert into Home Assistant statistics."""
        try:
            # Ensure we have a statistic_id (entity_id) set
            if not self._statistic_id:
                _LOGGER.debug("Skipping statistics update: entity_id not yet available")
                return
            
            counter_id = self._counter.get("meterCounterId")
            if not counter_id:
                _LOGGER.warning("No meterCounterId available for statistics sensor")
                return
            
            _LOGGER.debug("Updating statistics for counter: %s (statistic_id: %s)", 
                          counter_id, self._statistic_id)
            
            end_date = datetime.now(timezone.utc)
            
            # Check if we have existing statistics
            last_stats = await get_instance(self.hass).async_add_executor_job(
                get_last_statistics,
                self.hass,
                1,
                self._statistic_id,
                True,
                {"sum"},
            )
            
            has_existing_stats = bool(last_stats and self._statistic_id in last_stats)
            
            if has_existing_stats:
                # We have existing stats, only fetch recent data (last 7 days)
                start_date = end_date - timedelta(days=7)
                _LOGGER.debug("Existing statistics found, fetching last 7 days")
            else:
                # No existing stats, fetch last 30 days (API max is 31 days)
                start_date = end_date - timedelta(days=30)
                # Reset timestamp filter to allow all data
                self._last_inserted_timestamp = None
                _LOGGER.info("No existing statistics, fetching last 30 days of history")
            
            start_date_iso = start_date.strftime("%Y-%m-%dT00:00:00Z")
            end_date_iso = end_date.strftime("%Y-%m-%dT23:59:59Z")
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as session:
                response = await session.post(
                    f"{self._smarthome_base_url}/installations/{self._installation_id}/readings/historical",
                    json={"from": start_date_iso, "to": end_date_iso},
                    headers={"ApiKey": self._api_key, "Content-Type": "application/json"},
                )
                
                if response.status == 429:
                    _LOGGER.warning("Rate limit exceeded for statistics data, will retry later")
                    return
                
                # Handle 404/403 - installation no longer accessible
                if response.status in (404, 403):
                    _LOGGER.warning(
                        "Installation %s no longer accessible (HTTP %s)",
                        self._installation_id,
                        response.status
                    )
                    await self._handle_installation_unavailable()
                    return
                
                if not response.ok:
                    _LOGGER.error("Statistics data request failed: HTTP %s", response.status)
                    return
                
                historical_data = await response.json()
                readings = historical_data.get("readings", [])
                
                _LOGGER.debug("Historical API returned %d readings for installation %s", 
                              len(readings), self._installation_id)
                
                # Debug: Log sample reading to understand structure
                if readings and self._reading_type == "consumption":
                    sample = readings[0]
                    matching_samples = [r for r in readings[:5] if r.get("meterCounterId") == counter_id]
                    _LOGGER.debug("Sample reading keys: %s", list(sample.keys()))
                    _LOGGER.debug("Looking for meterCounterId=%s, found %d matches in first 5", 
                                  counter_id, len(matching_samples))
                    if matching_samples:
                        _LOGGER.debug("Matching sample: %s", matching_samples[0])
                
                # Filter readings for this counter
                # For both types: use value field (API always puts the reading in value)
                # - Counter type: value = cumulative meter reading
                # - Consumption type: value = interval consumption (need to accumulate)
                counter_readings = [
                    r for r in readings 
                    if r.get("meterCounterId") == counter_id and r.get("value") is not None
                ]
                
                _LOGGER.debug("Filtered to %d readings for counter %s (reading_type=%s)", 
                              len(counter_readings), counter_id, self._reading_type)
                
                if not counter_readings:
                    _LOGGER.debug("No readings found for counter %s (reading_type=%s)", counter_id, self._reading_type)
                    return
                
                # Sort by timestamp
                counter_readings.sort(key=lambda x: x.get("timestamp", ""))
                
                # For counter-type meters without existing statistics:
                # Use the first reading as a baseline and subtract it from all values
                # This prevents the Energy Dashboard from showing massive consumption (0 to current meter value)
                # Instead, consumption starts from 0 and only shows the delta from the first reading
                counter_baseline = None
                if self._reading_type != "consumption" and not has_existing_stats:
                    # Get the first reading value as baseline
                    if counter_readings:
                        first_value = counter_readings[0].get("value")
                        if first_value is not None:
                            if isinstance(first_value, str):
                                counter_baseline = float(re.sub(r'[^\d.-]', '', first_value.strip()))
                            else:
                                counter_baseline = float(first_value)
                            _LOGGER.debug(
                                "Using first counter reading as baseline: %s (will subtract from all values)",
                                counter_baseline
                            )
                
                # For consumption type, get existing sum from statistics to continue accumulating
                if self._reading_type == "consumption" and self._cumulative_sum == 0.0:
                    try:
                        existing_stats = await get_instance(self.hass).async_add_executor_job(
                            get_last_statistics,
                            self.hass,
                            1,
                            self._statistic_id,
                            True,
                            {"sum"},
                        )
                        if existing_stats and self._statistic_id in existing_stats:
                            last_stat = existing_stats[self._statistic_id][0]
                            self._cumulative_sum = last_stat.get("sum", 0.0) or 0.0
                            _LOGGER.debug("Loaded existing cumulative sum: %s", self._cumulative_sum)
                    except Exception as err:
                        _LOGGER.debug("Could not load existing statistics: %s", err)
                
                # Build statistics data
                statistics: list[StatisticData] = []
                cumulative_sum = self._cumulative_sum
                
                for reading in counter_readings:
                    try:
                        timestamp_str = reading.get("timestamp")
                        if not timestamp_str:
                            continue
                        
                        # Parse timestamp - handle both Z suffix and explicit timezone offsets
                        if timestamp_str.endswith("Z"):
                            timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                        else:
                            timestamp = datetime.fromisoformat(timestamp_str)
                        
                        # Convert to UTC for Home Assistant statistics
                        timestamp = timestamp.astimezone(timezone.utc)
                        
                        # Round to the start of the hour (in UTC)
                        timestamp = timestamp.replace(minute=0, second=0, microsecond=0)
                        
                        # For counter-type meters, the API timestamp represents WHEN the reading was taken
                        # (end of measurement period). We need to attribute it to the PREVIOUS hour.
                        # Example: reading at 00:05 represents the meter state at end of 23:00-00:00 hour
                        #          so it should be stored with start=23:00, not 00:00
                        # For consumption-type, the timestamp is already correct from the API.
                        if self._reading_type != "consumption":
                            timestamp = timestamp - timedelta(hours=1)
                        
                        # Skip if we already have this timestamp
                        if self._last_inserted_timestamp and timestamp <= self._last_inserted_timestamp:
                            continue
                        
                        if self._reading_type == "consumption":
                            # For consumption type: accumulate interval values to create virtual meter
                            # Note: API puts interval consumption in "value" field, not "consumption"
                            interval_value = reading.get("value")
                            if interval_value is None:
                                continue
                            
                            # Parse interval value
                            if isinstance(interval_value, str):
                                interval_value = float(re.sub(r'[^\d.-]', '', interval_value.strip()))
                            else:
                                interval_value = float(interval_value)
                            
                            # Only add positive consumption
                            if interval_value > 0:
                                cumulative_sum += interval_value
                            
                            statistics.append(
                                StatisticData(
                                    start=timestamp,
                                    state=interval_value,  # Current period consumption
                                    sum=cumulative_sum,    # Virtual meter reading (accumulated)
                                )
                            )
                        else:
                            # For counter type: value is already cumulative
                            value = reading.get("value")
                            if value is None:
                                continue
                            
                            # Parse value
                            if isinstance(value, str):
                                value = float(re.sub(r'[^\d.-]', '', value.strip()))
                            else:
                                value = float(value)
                            
                            # Calculate sum: if we have a baseline, subtract it to get relative consumption
                            # This ensures the Energy Dashboard starts from 0, not from the meter's total value
                            if counter_baseline is not None:
                                relative_sum = value - counter_baseline
                            else:
                                relative_sum = value
                            
                            statistics.append(
                                StatisticData(
                                    start=timestamp,
                                    state=value,  # Original meter reading
                                    sum=relative_sum,  # Relative to baseline (starts near 0)
                                )
                            )
                        
                    except (ValueError, TypeError) as err:
                        _LOGGER.debug("Error parsing reading: %s - %s", reading, err)
                        continue
                
                # Update cumulative sum for next time
                if self._reading_type == "consumption":
                    self._cumulative_sum = cumulative_sum
                
                if not statistics:
                    _LOGGER.debug("No new statistics to insert for counter %s", counter_id)
                    return
                
                # Import statistics into the sensor's history using its entity_id
                # This allows the Energy Dashboard to see the historical data
                if not self._statistic_id:
                    _LOGGER.warning("Cannot insert statistics: entity_id not set yet")
                    return
                
                # Create metadata - use "recorder" as source for sensor statistics
                metadata = StatisticMetaData(
                    has_mean=False,
                    has_sum=True,
                    mean_type=StatisticMeanType.NONE,
                    name=self.name or f"{self._base_device_name}",
                    source="recorder",
                    statistic_id=self._statistic_id,
                    unit_of_measurement=self._stat_unit,
                    unit_class=self._unit_class,
                )
                
                # Insert statistics using async_import_statistics
                # This imports into the sensor's existing statistics
                async_import_statistics(self.hass, metadata, statistics)
                
                # Update last inserted timestamp
                if statistics:
                    self._last_inserted_timestamp = statistics[-1]["start"]
                
                self._last_stats_update = datetime.now(timezone.utc)
                _LOGGER.info(
                    "Inserted %d statistics records for %s (from %s to %s)",
                    len(statistics),
                    self._statistic_id,
                    statistics[0]["start"].isoformat() if statistics else "N/A",
                    statistics[-1]["start"].isoformat() if statistics else "N/A",
                )
                
                # Update entity state for consumption-type meters to reflect new cumulative sum
                if self._reading_type == "consumption":
                    self.async_write_ha_state()
                
        except asyncio.TimeoutError:
            _LOGGER.warning("Timeout fetching statistics data for sensor")
        except aiohttp.ClientError as err:
            _LOGGER.error("Connection error fetching statistics data: %s", err)
        except Exception as err:
            _LOGGER.exception("Unexpected error updating statistics: %s", err)

    async def _update_from_meter_counters(self, meter_counters: list[dict]) -> None:
        """Not used for statistics sensor - we use async_update_statistics instead."""
        pass


def to_snake_case(s: str) -> str:
    """Convert a string to snake_case."""
    s = re.sub("[^a-zA-Z0-9]", " ", s)
    s = re.sub("(.)([A-Z][a-z]+)", r"\1 \2", s)
    s = re.sub("([a-z0-9])([A-Z])", r"\1 \2", s)
    return s.lower().replace(" ", "_")
