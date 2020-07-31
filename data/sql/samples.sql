SELECT
s.id, s.device_id, s.`timestamp`, s.battery_state, (s.battery_level * 100) AS battery_level, s.timezone, s.country_code,
bd.charger, bd.health, bd.voltage, bd.temperature,
cs.`usage`, cs.up_time, cs.sleep_time,
s.network_status, nd.mobile_data_status, nd.mobile_data_activity, nd.wifi_status, nd.wifi_signal_strength, nd.wifi_link_speed,
s.screen_on, s.screen_brightness, nd.roaming_enabled, s2.bluetooth_enabled, s2.location_enabled, s2.power_saver_enabled, s2.nfc_enabled, s2.developer_mode,
sd.`free`, sd.total, sd.free_system, sd.total_system
FROM samples s
LEFT JOIN battery_details bd ON bd.sample_id = s.id
LEFT JOIN cpu_statuses cs ON cs.sample_id = s.id
LEFT JOIN network_details nd ON nd.sample_id = s.id
LEFT JOIN settings s2 ON s2.sample_id = s.id
LEFT JOIN storage_details sd on sd.sample_id = s.id;
ORDER BY s.id;